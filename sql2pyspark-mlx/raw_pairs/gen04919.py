from pyspark.sql import functions as F

result = (
    df_orders
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["country", "city"], ["country"], []],
        F.col("c.country").alias("country"),
        F.col("c.city").alias("city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.amount")).alias("total_amount"),
        F.max(F.col("a.discount")).alias("max_discount"),
        F.grouping_id().alias("level_id"),
    )
)
