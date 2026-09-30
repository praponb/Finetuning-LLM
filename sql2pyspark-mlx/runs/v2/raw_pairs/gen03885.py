from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("last_restock") < "2023-03-07")
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("pt.quantity_range"),
    )
    .orderBy("product_name")
)
