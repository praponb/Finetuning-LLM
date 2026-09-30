from pyspark.sql import functions as F

kept_keys = (
    df_subscriptions
    .select("id", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status") == "EXPIRED")
        .select("id", "plan")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("monthly_fee") >= 195, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee_2"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
