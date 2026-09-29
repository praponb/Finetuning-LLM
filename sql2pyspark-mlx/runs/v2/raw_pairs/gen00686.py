from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.upper(F.trim("name")) == "ALICE WONG").alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.shipping_fee"))
)
