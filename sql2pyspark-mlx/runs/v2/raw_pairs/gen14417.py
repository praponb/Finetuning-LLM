from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.signup_date") >= "2023-08-30")
    .groupBy(F.col("s.city"))
    .agg(
        F.count("*").alias("row_count"),
        F.min(F.col("o.order_date")).alias("min_order_date"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("row_count").desc())
)
