from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("last_restock").between("2025-07-01", "2025-07-15"))
    .withColumn("last_restock_quarter", F.quarter("last_restock"))
)

summary = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    summary
    .select("product_id", "avg_quantity", "total_quantity")
    .orderBy(F.col("total_quantity").desc())
)
