from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 13)
    .select("id", "shipping_fee", "store_id", F.trim("status").alias("clean_status"))
    .orderBy("id", F.col("clean_status").desc())
)
