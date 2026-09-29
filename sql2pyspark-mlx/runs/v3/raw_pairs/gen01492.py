from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 3200) & (F.col("revenue") <= 3100))
    .select("store_id")
)
