from pyspark.sql import functions as F

dim_totals = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.segment").alias("segment"),
        F.col("p.country").alias("country"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.monthly_fee")).alias("max_monthly_fee"),
    )
)

result = (
    dim_totals
    .select(
        "segment",
        "country",
        "status",
        "max_monthly_fee",
        F.when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "CORE").when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("status")
)
