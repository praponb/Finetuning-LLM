from pyspark.sql import functions as F

rollup_totals = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.segment").alias("segment"),
        F.col("d.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select(
        "segment",
        "status",
        "cnt",
        "max_monthly_fee",
        F.when(F.col("segment").rlike("^(SMB|Corporate)$"), "EXTENDED").when(F.col("segment").rlike("^(Consumer)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("segment_zone"),
    )
    .orderBy(F.col("segment").desc_nulls_last())
)
