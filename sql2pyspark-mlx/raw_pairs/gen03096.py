from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("order_id").isin([16, 34]) & (F.col("status") == "RETURNED"))
    .withColumn("carrier_title", F.initcap(F.trim("carrier")))
)

summary = (
    src
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
)

result = summary.select("shipped_month", "avg_cost")
