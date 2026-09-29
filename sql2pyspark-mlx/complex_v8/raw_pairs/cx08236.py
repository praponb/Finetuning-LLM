from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_month_by_st = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id")
w_product_end_date_month_by_st_2 = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_end_date_month_by_st_3 = Window.partitionBy("product", F.date_trunc("month", "end_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_policies
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.rank().over(w_product_end_date_month_by_st).alias("start_date_rank"),
        F.last("end_date", ignorenulls=True).over(w_product_end_date_month_by_st_2).alias("last_known_end_date"),
        F.round(F.percent_rank().over(w_product_end_date_month_by_st), 3).alias("start_date_percent_rank"),
        F.first("start_date").over(w_product_end_date_month_by_st_3).alias("first_start_date"),
        F.dense_rank().over(w_product_end_date_month_by_st).alias("start_date_dense_rank"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

profiled = (
    metrics.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status_prefix",
        "product_prefix",
        "start_date_rank",
        "last_known_end_date",
        "start_date_percent_rank",
        "first_start_date",
        "start_date_dense_rank",
        "claim_count",
    )
    .orderBy(F.col("start_date_dense_rank").asc())
)
