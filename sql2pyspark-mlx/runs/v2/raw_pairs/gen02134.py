from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("quantity") > 150) & (F.col("last_restock") >= "2025-09-02"))
)

summary = (
    base
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    summary
    .filter(F.col("total_quantity") < 720)
    .select("product_id", "total_quantity")
    .orderBy(F.col("product_id").desc())
)
