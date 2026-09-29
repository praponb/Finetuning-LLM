from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_segment_end_date_mont = Window.partitionBy("holder_segment", F.date_trunc("month", "end_date")).orderBy("start_date")
w_product_holder_id_by_start_d = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc(), "id")
w_start_date_month_holder_id_b = Window.partitionBy(F.month("start_date"), "holder_id").orderBy("start_date", "id")
w_product_holder_id_by_start_d_2 = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_holder_id_by_start_d_3 = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc())
w_holder_segment_by_end_date_t = Window.partitionBy("holder_segment").orderBy("end_date")
w_product_by_end_date = Window.partitionBy("product").orderBy("end_date", "id")
w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.month("end_date")).orderBy(F.col("start_date").asc_nulls_last(), "id")
w_holder_segment_end_date_mont_2 = Window.partitionBy("holder_segment", F.date_trunc("month", "end_date")).orderBy("start_date", "id")
w_holder_segment_by_end_date = Window.partitionBy("holder_segment").orderBy("end_date", "id")
w_start_date_month_holder_id_b_2 = Window.partitionBy(F.month("start_date"), "holder_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_holder_id_by_start_d_4 = Window.partitionBy("product", "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(" / ", F.col("product"), F.col("status")).alias("product_status_label"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
    )
)

combined_details = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.end_date_label",
        F.col("p.segment").alias("holder_segment"),
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "product",
        "start_date",
        F.rank().over(w_holder_segment_end_date_mont).alias("start_date_rank"),
        F.dense_rank().over(w_product_holder_id_by_start_d).alias("start_date_dense_rank"),
        F.lag("end_date").over(w_start_date_month_holder_id_b).alias("prev_end_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_start_date_month_holder_id_b)).alias("days_since_prev_start_date"),
        F.first("start_date").over(w_product_holder_id_by_start_d_2).alias("first_start_date"),
        F.round(F.cume_dist().over(w_product_holder_id_by_start_d_3), 3).alias("start_date_cume_dist"),
        F.round(F.cume_dist().over(w_holder_segment_by_end_date_t), 3).alias("end_date_cume_dist"),
        F.lead("start_date", 2).over(w_product_by_end_date).alias("next2_start_date"),
        F.lead("start_date", 3).over(w_holder_id_end_date_month_by).alias("next3_start_date"),
        F.lag("start_date", 2).over(w_holder_segment_end_date_mont_2).alias("prev2_start_date"),
        F.round(F.cume_dist().over(w_holder_segment_by_end_date), 3).alias("end_date_cume_dist_2"),
        F.first("start_date", ignorenulls=True).over(w_start_date_month_holder_id_b_2).alias("first_known_start_date"),
        F.round(F.percent_rank().over(w_holder_id_end_date_month_by), 3).alias("start_date_percent_rank"),
        F.last("start_date", ignorenulls=True).over(w_product_holder_id_by_start_d_4).alias("last_known_start_date"),
    )
)

first_rows = (
    windowed
    .filter((F.col("start_date_rank") <= 5) & (F.col("start_date_dense_rank") <= 7))
    .drop("start_date_rank")
)

result = (
    first_rows
    .select(
        "id",
        "product",
        "start_date",
        "start_date_dense_rank",
        "prev_end_date",
        "first_start_date",
        "start_date_cume_dist",
        "next3_start_date",
        "prev2_start_date",
        "first_known_start_date",
        "last_known_start_date",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
    .orderBy(
        F.col("last_known_start_date").asc_nulls_last(),
        F.col("start_date_dense_rank").desc(),
        "id",
    )
    .limit(10)
)
