from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product = Window.partitionBy("holder_id", "product")
w_status_by_holder_joined_on = Window.partitionBy("status").orderBy(F.col("holder_joined_on").desc(), "id")
w_status_by_holder_joined_on_r = Window.partitionBy("status").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_holder_joined_on_f = Window.partitionBy("status").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_by_premium_r = Window.partitionBy(F.year("start_date")).orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_year_by_premium = Window.partitionBy(F.year("start_date")).orderBy(F.col("premium").desc(), "id")
w_status_by_holder_joined_on_m = Window.partitionBy("status").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-6, Window.currentRow)
w_holder_id = Window.partitionBy("holder_id")
w_start_date_year_by_premium_t = Window.partitionBy(F.year("start_date")).orderBy(F.col("premium").desc())
w_product_by_end_date_ties = Window.partitionBy("product").orderBy("end_date")
w_product_by_end_date = Window.partitionBy("product").orderBy("end_date", "id")
w_start_date_year_by_premium_f = Window.partitionBy(F.year("start_date")).orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-12-05").cast("date")) & (F.col("premium") < 3465.4))
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
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"))
)

windowed = (
    with_details
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "holder_joined_on",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_product), F.lit(0)), 4).alias("premium_share_of_holder_id_product"),
        F.lag("end_date").over(w_status_by_holder_joined_on).alias("prev_end_date"),
        F.count(F.lit(1)).over(w_status_by_holder_joined_on_r).alias("running_count"),
        F.last("holder_joined_on").over(w_status_by_holder_joined_on_f).alias("last_holder_joined_on"),
        F.first("holder_joined_on", ignorenulls=True).over(w_start_date_year_by_premium_r).alias("first_known_holder_joined_on"),
        F.row_number().over(w_start_date_year_by_premium).alias("rn"),
        F.dense_rank().over(w_status_by_holder_joined_on).alias("holder_joined_on_dense_rank"),
        F.sum("premium").over(w_status_by_holder_joined_on_m).alias("premium_sum_7"),
        F.round(F.avg("premium").over(w_holder_id_product), 2).alias("holder_id_product_avg_premium"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product), 2).alias("premium_vs_holder_id_product_avg"),
        F.round(F.percent_rank().over(w_start_date_year_by_premium_t), 3).alias("premium_percent_rank"),
        F.dense_rank().over(w_product_by_end_date_ties).alias("end_date_dense_rank"),
        F.round(F.cume_dist().over(w_product_by_end_date), 3).alias("end_date_cume_dist"),
        F.first("end_date").over(w_start_date_year_by_premium_f).alias("first_end_date"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "premium_share_of_holder_id_product",
        "first_known_holder_joined_on",
        "rn",
        "holder_joined_on_dense_rank",
        "premium_sum_7",
        "holder_id_product_avg_premium",
        "holder_id_max_premium",
        "premium_vs_holder_id_product_avg",
        "end_date_cume_dist",
        "first_end_date",
        F.date_format(F.col("last_holder_joined_on"), "yyyy-MM-dd").alias("last_holder_joined_on_label"),
    )
    .orderBy("rn", F.col("premium").desc_nulls_first(), "id")
    .limit(5)
)
