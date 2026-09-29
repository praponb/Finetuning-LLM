from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("quantity") == 83) & F.month("last_restock").isin([4, 6, 11]) & (F.col("quantity") >= 189)))
    .groupBy(
        "product_id",
        "store_id",
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") < 160)
    .orderBy("store_id", F.col("product_id").desc())
)
