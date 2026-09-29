from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
    .orderBy(F.col("last_restock_weekofyear").desc(), F.col("last_restock").desc())
)
