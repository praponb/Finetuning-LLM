from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(62, 90) & ((F.col("product_id") == 2) | F.month("last_restock").isin([1, 3, 8])))
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 680)
    .drop("total_quantity")
)
