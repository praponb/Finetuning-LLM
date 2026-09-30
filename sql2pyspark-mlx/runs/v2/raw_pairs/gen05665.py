from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .filter(F.col("products.category").isin(["Home", "Grocery", "Books"]) & F.col("order_items.order_id").isin([24, 31]))
    .select(
        F.col("products.category"),
        F.col("products.price"),
        F.col("products.brand"),
        F.col("order_items.product_id"),
    )
)
