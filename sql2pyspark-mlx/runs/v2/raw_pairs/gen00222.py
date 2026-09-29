from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1320)
    .select("amount", "store_id")
    .orderBy("store_id")
)
