from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "product_id",
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") >= 150)
    .orderBy(F.col("product_id").desc(), "last_restock_month")
)
