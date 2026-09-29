from pyspark.sql import functions as F

rollup_totals = (
    df_orders
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.country").alias("country"), F.col("s.channel").alias("channel"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.shipping_fee")).alias("max_shipping_fee"),
        F.max(F.col("s.discount")).alias("max_discount"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select("country", "channel", "cnt", "max_shipping_fee", "max_discount")
    .orderBy(F.col("country").desc())
)
