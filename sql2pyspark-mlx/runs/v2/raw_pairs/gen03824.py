from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.city").isin(["Tokyo", "Berlin", "Singapore"]))
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.when(F.col("o.channel") == "web", 1)).alias("web_count"),
        F.count("*").alias("num_orders"),
    )
    .orderBy(F.col("country").desc())
)
