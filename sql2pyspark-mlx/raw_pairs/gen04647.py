from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        "quantity",
        "product_id",
        F.col("quantity").cast("double").alias("quantity_num"),
        F.round((F.col("quantity") * 1.15), 2).alias("adjusted_quantity"),
    )
    .orderBy(F.col("adjusted_quantity").desc())
)
