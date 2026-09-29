from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 6]))
    .select("id", "quantity")
    .orderBy("id", F.col("quantity").desc())
    .limit(20)
)
