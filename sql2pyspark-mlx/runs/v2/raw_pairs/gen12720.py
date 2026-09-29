from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter((F.col("oi.product_id") == 20) & F.col("oi.product_id").isNotNull())
    .select(
        F.col("oi.quantity"),
        F.col("p.launch_date"),
        F.greatest(F.col("p.price"), F.col("p.cost")).alias("greatest_price_cost"),
    )
)
