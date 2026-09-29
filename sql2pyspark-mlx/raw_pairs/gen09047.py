from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.year("last_restock").alias("last_restock_year"), "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("max_quantity") >= 70)
    .select("last_restock_year", "product_id", "max_quantity", "quantity_count")
    .orderBy(F.col("max_quantity").desc(), "last_restock_year", "product_id")
    .limit(5)
)
