from pyspark.sql import functions as F

result = (
    df_orders
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .cube(
        F.col("c.country").alias("country"),
        F.col("c.segment").alias("segment"),
        F.col("a.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.discount")).alias("avg_discount"),
        F.avg(F.col("a.shipping_fee")).alias("avg_shipping_fee"),
    )
)
