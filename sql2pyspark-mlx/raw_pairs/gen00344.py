from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 84)
    .select("id", "shipped_date", F.substring("status", 1, 4).alias("status_prefix"))
    .orderBy("status_prefix")
)
