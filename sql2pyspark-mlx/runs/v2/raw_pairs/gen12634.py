from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 640)
    .groupBy("supplier_id", F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
        F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
    )
)
