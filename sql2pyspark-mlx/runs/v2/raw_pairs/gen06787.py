from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2025-05-01", "2025-06-29") & F.col("product_id").isin([6, 8]))
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
    )
)
