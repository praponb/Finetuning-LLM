from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.count("*").alias("num_inventory"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("pt.first_last_restock"),
        F.col("pt.num_inventory"),
    )
    .orderBy(F.col("num_inventory").desc())
)
