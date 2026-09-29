from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-08-10", "2024-10-01") & (F.col("quantity") <= 111))
    .select(
        "store_id",
        "product_id",
        "quantity",
        F.round((F.col("quantity") * 100), 2).alias("adjusted_quantity"),
    )
    .orderBy("quantity", "product_id")
)
