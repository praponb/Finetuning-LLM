from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
)

agg = (
    src
    .groupBy("status")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    agg
    .select("status", "avg_cost", "min_cost")
    .orderBy("avg_cost")
)
