from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & (F.col("shipped_date") > "2023-11-01"))
    .groupBy("carrier", "status")
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") >= 2)
)
