from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "store_id")
    .orderBy(F.col("store_id").desc())
)
