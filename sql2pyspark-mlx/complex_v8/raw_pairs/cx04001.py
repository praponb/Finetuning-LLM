from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_product_by_pr = Window.partitionBy(F.date_trunc("month", "end_date"), "product").orderBy("premium", "id")
w_end_date_month = Window.partitionBy("end_date_month")

filtered = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.month(F.col("end_date")).alias("end_date_month"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

sequenced = (
    summarized_rows
    .withColumn("premium_quintile", F.ntile(5).over(w_end_date_month_product_by_pr))
    .withColumn("distinct_status_in_end_date_month", F.size(F.collect_set("status").over(w_end_date_month)))
)

status_stats = (
    sequenced
    .groupBy("status")
    .agg(F.max("premium_bucket").alias("max_premium_bucket"))
)

above_average = (
    sequenced.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium_bucket") == F.col("g.max_premium_bucket"))
    .select(
        "s.*",
        "g.max_premium_bucket",
        F.round(F.col("s.premium_bucket") - F.col("g.max_premium_bucket"), 2).alias("premium_bucket_minus_max"),
    )
)

result = (
    above_average
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "end_date_month",
        "premium_bucket",
        "distinct_status_in_end_date_month",
        "max_premium_bucket",
        F.round(F.col("premium_bucket_minus_max") * 1.07, 2).alias("premium_bucket_minus_max_adj"),
        F.round(F.col("premium_quintile") / F.nullif(F.col("premium_bucket_minus_max"), F.lit(0)), 3).alias("premium_quintile_per_premium_bucket_minus_max"),
    )
    .orderBy(F.col("end_date").desc(), "max_premium_bucket")
)
