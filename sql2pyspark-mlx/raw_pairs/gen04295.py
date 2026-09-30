from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
        "carrier",
    )
    .agg(F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"))
    .orderBy("delivered_month", "carrier")
    .limit(5)
)
