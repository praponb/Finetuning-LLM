from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("avg_cost") > 56)
    .select("status", "delivered_month", "avg_cost", "min_cost")
    .orderBy("min_cost")
)
