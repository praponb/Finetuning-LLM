from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["segment", "city", "status"], ["segment", "city"], ["segment"], []],
        F.col("d.segment").alias("segment"),
        F.col("d.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.monthly_fee")).alias("min_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
