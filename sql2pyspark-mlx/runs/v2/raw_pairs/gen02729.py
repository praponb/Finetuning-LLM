from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select("carrier", "status", "shipped_date", F.trim("status").alias("clean_status"))
    .orderBy("carrier")
)
