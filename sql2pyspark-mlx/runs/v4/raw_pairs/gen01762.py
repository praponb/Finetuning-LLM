from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.city") == "Berlin")
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        F.min(F.col("o.amount")).alias("min_amount"),
        F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .orderBy(F.col("min_amount").desc(), F.col("city").desc())
)
