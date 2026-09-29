from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2024)
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.min("delivered_date").alias("first_delivered_date"),
    )
    .filter(F.col("flash_count") >= 3)
)
