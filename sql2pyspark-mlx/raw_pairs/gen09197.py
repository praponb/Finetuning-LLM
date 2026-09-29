from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status") == "EXPIRED", 1).otherwise(0).alias("is_expired"),
        F.length("status").alias("status_length"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "country"], ["segment"], []],
        F.col("c.segment").alias("segment"),
        F.col("c.country").alias("country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.is_expired")).alias("total_is_expired"),
        F.min(F.col("a.status_length")).alias("min_status_length"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
