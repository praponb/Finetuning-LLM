from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 3) & (F.col("quantity") != 73))
    .select(
        "product_id",
        "quantity",
        "store_id",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_scaled"),
    )
    .orderBy(F.col("product_id").desc())
)
