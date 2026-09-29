from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
    .orderBy(F.col("id").desc())
)
