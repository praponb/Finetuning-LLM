from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") >= 52)
    .orderBy(F.col("quantity_range").desc(), "product_id")
    .limit(50)
)
