from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2025)
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
    .filter(F.col("dhl_cost") >= 57)
    .select("delivered_month", "stddev_cost", "dhl_cost")
    .orderBy("dhl_cost")
)
