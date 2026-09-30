from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNull() & F.col("store_id").isin([1, 2, 4]))
    .select("store_id")
    .orderBy(F.col("store_id").desc())
)
