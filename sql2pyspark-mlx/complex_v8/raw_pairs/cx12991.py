from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")

valid = (
    df_policies
    .filter(F.col("holder_id") >= 9)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

sequenced = (
    valid
    .withColumn("rows_in_product", F.count(F.lit(1)).over(w_product))
    .withColumn("premium_share_of_product", F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4))
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

profiled = (
    sequenced.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

totals = (
    profiled
    .groupBy(
        "status",
        "product",
        F.date_trunc("month", "start_date").alias("start_date_month"),
    )
    .agg(
        F.sum("premium").alias("total_premium"),
        F.round(100 * F.count(F.when(F.col("product").isin("HEALTH", "TRAVEL", "AUTO"), 1)) / F.count("*"), 1).alias("pct_product_in_health_travel_aut"),
        F.round(F.sum("premium") / F.nullif(F.sum("premium_share_of_product"), F.lit(0)), 3).alias("premium_to_premium_share_of_product_ratio"),
        F.count("*").alias("cnt"),
        F.countDistinct("product").alias("distinct_products"),
        F.sum(F.when(~F.col("status").isin("EXPIRED", "ACTIVE"), 1).otherwise(0)).alias("n_status_not_in_expired_active"),
    )
)

result = (
    totals
    .select(
        "status",
        "start_date_month",
        "total_premium",
        "distinct_products",
        "n_status_not_in_expired_active",
    )
    .orderBy(F.col("start_date_month").desc(), F.col("total_premium").desc())
)
