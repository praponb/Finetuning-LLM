from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("p.price"),
    )
)
