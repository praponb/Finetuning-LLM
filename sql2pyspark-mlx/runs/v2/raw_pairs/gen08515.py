from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2023-05-01")
    .groupBy(
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
        "status",
    )
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
    .orderBy("first_delivered_date", F.col("kerry_count").desc())
)
