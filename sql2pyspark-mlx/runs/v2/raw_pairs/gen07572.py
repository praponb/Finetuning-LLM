from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("cost").isNull() & F.col("cost").isNull())
    .withColumn("delivered_date_year", F.year("delivered_date"))
)

summary = (
    filtered_shipments
    .groupBy("carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("shipped_date").alias("last_shipped_date"),
    )
)

result = summary
