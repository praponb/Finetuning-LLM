from pyspark.sql import functions as F

rollup_totals = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "city", "status"], ["segment"]],
        F.col("c.segment").alias("segment"),
        F.col("c.city").alias("city"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.balance")).alias("max_balance"),
    )
)

result = (
    rollup_totals
    .select(
        "segment",
        "status",
        "cnt",
        "max_balance",
        F.when(F.col("segment").rlike("^(Corporate|Consumer)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("segment_zone"),
    )
    .orderBy(F.col("max_balance").desc_nulls_last(), "cnt")
)
