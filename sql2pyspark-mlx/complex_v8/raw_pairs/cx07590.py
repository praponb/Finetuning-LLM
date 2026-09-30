from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id_read_at_month = Window.partitionBy("site_id", F.date_trunc("month", "read_at"))
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_all_by_read_at_prior_2 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_prior_1 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-1, -1)

cleaned = (
    df_meter_readings
    .filter((F.col("voltage") < 241.9) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "quality_clean",
        F.rank().over(w_all_by_read_at).alias("read_at_rank"),
        F.count(F.lit(1)).over(w_site_id_read_at_month).alias("rows_in_site_id_read_at_month"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.max("kwh_bucket").over(w_all_by_read_at_prior_2).alias("kwh_bucket_max_2_prior"),
        F.count(F.lit(1)).over(w_all_by_read_at_running).alias("running_count"),
        F.sum("kwh_bucket").over(w_all_by_read_at_prior_1).alias("kwh_bucket_sum_1_prior"),
    )
)

derived = (
    ranked
    .select(
        "id",
        "kwh",
        "quality_clean",
        "read_at_rank",
        "rows_in_site_id_read_at_month",
        "running_count",
        "kwh_bucket_sum_1_prior",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.round(F.col("kwh_bucket_max_2_prior") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("kwh_bucket_max_2_prior_per_kwh"),
    )
)

result = (
    derived
    .select(
        "id",
        "quality_clean",
        "read_at_rank",
        "rows_in_site_id_read_at_month",
        "running_count",
        "kwh_bucket_sum_1_prior",
        "quality_len",
        "kwh_bucket_max_2_prior_per_kwh",
        F.length(F.trim(F.col("quality_clean"))).alias("quality_clean_len"),
    )
    .orderBy("read_at_rank")
)
