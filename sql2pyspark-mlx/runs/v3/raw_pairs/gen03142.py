from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("last_restock") >= "2023-11-01") & F.col("quantity").isNull()) | (F.col("last_restock") > "2023-09-01"))
    .select(
        "store_id",
        "quantity",
        "last_restock",
        F.month("last_restock").alias("last_restock_month"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
    .orderBy(F.col("quantity").desc())
)
