from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_segment_by_en = Window.partitionBy("product", "holder_segment").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_status_by_sta = Window.partitionBy("holder_segment", "status").orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_product_holder_segment_by_en_2 = Window.partitionBy("product", "holder_segment").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_holder_id_by_end_date_runnin = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_segment_by_en_3 = Window.partitionBy("product", "holder_segment").orderBy("end_date", "id")
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy("end_date")
w_product_by_end_date_full = Window.partitionBy("product").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_start_date_month_by_l = Window.partitionBy(F.date_trunc("month", "second_start_date")).orderBy("last_known_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_prev_end_date = Window.partitionBy("holder_segment", F.month("prev_end_date")).orderBy("second_start_date")
w_rn_2_by_end_date_running = Window.partitionBy("rn_2").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_end_date_year = Window.partitionBy(F.year("prev_end_date"))

prepared_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("start_date") >= F.lit("2024-08-27").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_details = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "product",
        "start_date",
        "end_date",
        "status",
        "holder_segment",
        F.first("end_date", ignorenulls=True).over(w_product_holder_segment_by_en).alias("first_known_end_date"),
        F.last("end_date").over(w_holder_segment_status_by_sta).alias("last_end_date"),
        F.first("end_date").over(w_holder_id_by_end_date_full).alias("first_end_date"),
        F.row_number().over(w_holder_id_by_end_date).alias("rn"),
        F.first("start_date").over(w_product_holder_segment_by_en_2).alias("first_start_date"),
        F.lag("end_date").over(w_all_by_start_date).alias("prev_end_date"),
        F.last("end_date", ignorenulls=True).over(w_holder_id_by_end_date_runnin).alias("last_known_end_date"),
        F.lead("start_date").over(w_all_by_start_date).alias("next_start_date"),
        F.nth_value("start_date", 2).over(w_product_holder_segment_by_en_2).alias("second_start_date"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_product_holder_segment_by_en_3)).alias("days_since_prev_end_date"),
        F.rank().over(w_all_by_start_date).alias("start_date_rank"),
        F.round(F.percent_rank().over(w_holder_id_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.row_number().over(w_all_by_start_date).alias("rn_2"),
        F.first("end_date").over(w_product_by_end_date_full).alias("first_end_date_2"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "product",
        "status",
        "holder_segment",
        "first_known_end_date",
        "last_end_date",
        "rn",
        "first_start_date",
        "prev_end_date",
        "last_known_end_date",
        "next_start_date",
        "end_date_percent_rank",
        F.first("end_date_percent_rank", ignorenulls=True).over(w_second_start_date_month_by_l).alias("first_known_end_date_percent_rank"),
        F.round(F.percent_rank().over(w_holder_segment_prev_end_date), 3).alias("second_start_date_percent_rank"),
        F.max("days_since_prev_end_date").over(w_rn_2_by_end_date_running).alias("max_days_since_prev_end_date_so_far"),
        F.round(F.avg("rn_2").over(w_prev_end_date_year), 2).alias("prev_end_date_year_avg_rn_2"),
        F.rank().over(w_holder_segment_prev_end_date).alias("second_start_date_rank"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "status",
        "holder_segment",
        "first_known_end_date",
        "prev_end_date",
        "last_known_end_date",
        "next_start_date",
        "end_date_percent_rank",
        "first_known_end_date_percent_rank",
        "second_start_date_percent_rank",
        "max_days_since_prev_end_date_so_far",
        "prev_end_date_year_avg_rn_2",
        "second_start_date_rank",
        F.concat(F.col("holder_segment"), F.lit("#"), F.col("rn").cast("string")).alias("holder_segment_ref"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
    .orderBy(
        F.col("second_start_date_percent_rank").asc_nulls_last(),
        F.col("max_days_since_prev_end_date_so_far").asc(),
        "id",
    )
    .limit(10)
)
