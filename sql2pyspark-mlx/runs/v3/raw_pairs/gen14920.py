from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .filter(F.col("total_quantity") >= 1250)
    .select("product_id", "last_restock_month", "total_quantity", "first_last_restock")
    .orderBy(F.col("first_last_restock").desc())
)
