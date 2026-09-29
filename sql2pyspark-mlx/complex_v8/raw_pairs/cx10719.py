from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter(F.col("premium") > 1042.8)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.abs(F.col("premium") - 3532.7).alias("premium_distance"),
    )
)

enriched = (
    base_policys
    .filter(F.col("end_date") < F.lit("2026-12-05").cast("date"))
    .withColumn("premium_distance_distance", F.abs(F.col("premium_distance") - 3500.0))
)

grouped = (
    enriched
    .groupBy("holder_id")
    .agg(
        F.round(F.sum("premium_distance"), 2).alias("total_premium_distance"),
        F.round(F.avg("premium_distance"), 2).alias("avg_premium_distance"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.round(100 * F.count(F.when(F.col("product") != "HEALTH", 1)) / F.count("*"), 1).alias("pct_product_health"),
        F.min("premium_distance_distance").alias("min_premium_distance_distance"),
    )
)

with_activity = (
    grouped.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "TRAVEL") & (F.col("p.end_date").isNotNull()), "left_anti")
)

detail_rows = (
    with_activity.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.holder_id",
        "s.total_premium_distance",
        "s.avg_premium_distance",
        "s.pct_product_health",
        "s.min_premium_distance_distance",
        F.col("p.id").alias("policy_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    detail_rows
    .filter(F.col("policy_status") != "CANCELLED")
    .select(
        "holder_id",
        "total_premium_distance",
        "avg_premium_distance",
        "pct_product_health",
        "min_premium_distance_distance",
        "policy_end_date",
        "policy_status",
        F.when(F.col("pct_product_health") >= 85.0, "large").when(F.col("pct_product_health") >= 30.0, "medium").otherwise("small").alias("pct_product_health_tier"),
        F.round(F.col("total_premium_distance") * 0.25, 2).alias("total_premium_distance_adj"),
    )
    .orderBy(
        F.col("policy_status").asc(),
        F.col("total_premium_distance").desc_nulls_first(),
    )
)
