from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select("method", F.lit("payment").alias("kind"))
    .union(df_stores.select("store_name", F.lit("store").alias("kind")))
)
