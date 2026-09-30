from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 1200) & (F.col("store_id") == 5))
    .select("store_id")
    .orderBy(F.col("store_id").desc())
)
