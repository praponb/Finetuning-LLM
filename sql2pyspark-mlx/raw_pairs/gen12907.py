from pyspark.sql import functions as F

by_dimension = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.segment").alias("segment"), F.col("a.plan").alias("plan"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.monthly_fee")).alias("max_monthly_fee"),
    )
)

result = (
    by_dimension
    .select(
        "segment",
        "plan",
        "rows_total",
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "EXTENDED").when(F.col("segment").rlike("^(Corporate)$"), "CORE").otherwise("GROUP_B").alias("segment_zone"),
    )
    .orderBy("plan", "segment_zone")
)
