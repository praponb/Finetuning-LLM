from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") >= 2024)
    .groupBy(
        "carrier",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)
