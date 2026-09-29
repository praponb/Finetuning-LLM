from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter((F.col("o.channel") == "store") & ~F.col("o.channel").isin(["store", "web"]))
    .select(
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("c.name"),
        F.col("c.is_active"),
        F.when(F.col("c.country") == "TH", 1).otherwise(0).alias("is_th"),
    )
    .orderBy(F.col("is_th").desc(), F.col("store_id").desc())
)
