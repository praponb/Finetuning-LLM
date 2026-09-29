from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") != "Flash") & F.col("order_id").isNull())
    .groupBy("status")
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)
