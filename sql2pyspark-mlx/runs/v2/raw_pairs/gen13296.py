from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([6, 7, 9]) & F.month("last_restock").isin([1, 5, 10]))
    .select("id", "quantity", F.col("quantity").cast("double").alias("quantity_num"))
    .orderBy(F.col("id").desc())
)
