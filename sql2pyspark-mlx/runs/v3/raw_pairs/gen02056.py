from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("quantity_range") < 33)
    .select("product_id", "quantity_range", "min_last_restock")
    .orderBy(F.col("min_last_restock").desc(), "product_id")
    .limit(20)
)
