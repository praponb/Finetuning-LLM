from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter((F.col("p.price") < 770) & F.col("i.quantity").between(81, 103))
    .select(
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.price"),
    )
    .orderBy("last_restock")
)
