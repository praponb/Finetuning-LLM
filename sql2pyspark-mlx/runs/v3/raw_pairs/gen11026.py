from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("status") == "IN_TRANSIT") | F.col("cost").between(32, 84))
    .withColumn("days_since_delivered_date", F.datediff(F.current_date(), "delivered_date"))
)

carrier_summary = (
    valid_shipments
    .groupBy("carrier")
    .agg(
        F.count("days_since_delivered_date").alias("days_since_delivered_date_count"),
        F.countDistinct("cost").alias("unique_orders"),
    )
)

result = (
    carrier_summary
    .filter(F.col("unique_orders") < 7)
    .select("carrier", "days_since_delivered_date_count", "unique_orders")
    .orderBy(F.col("days_since_delivered_date_count").desc(), "carrier")
    .limit(10)
)
