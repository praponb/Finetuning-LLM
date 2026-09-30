from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.status"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("c.age"),
        F.col("c.country"),
        F.substring(F.col("c.name"), 1, 4).alias("name_prefix"),
    )
    .orderBy(F.col("discount").desc())
)
