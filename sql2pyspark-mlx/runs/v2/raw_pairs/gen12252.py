from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") < 187) & F.month("last_restock").isin([7, 9, 10]) & (F.col("last_restock") > "2024-09-01"))
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc())
)
