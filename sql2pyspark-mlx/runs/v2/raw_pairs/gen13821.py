from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 7)
    .select(
        "id",
        "last_restock",
        "quantity",
        "store_id",
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
        F.col("quantity").cast("double").alias("quantity_num"),
    )
    .orderBy(F.col("last_restock_weekofyear").desc(), "id")
    .limit(5)
)
