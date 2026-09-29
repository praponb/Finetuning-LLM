from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("customer_id", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("plan") != "basic")
        .select("customer_id", "id")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("monthly_fee") >= 257, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("monthly_fee_per_premium"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("enterprise_count").asc_nulls_last())
)
