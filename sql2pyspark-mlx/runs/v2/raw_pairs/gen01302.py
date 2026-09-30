from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.upper(F.trim(F.col("c.email"))) == "EMMA@OUTLOOK.COM")
    .groupBy(
        F.when(F.col("o.amount") >= 1460, "high").when(F.col("o.amount") >= 540, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
    .orderBy("max_unit_price")
)
