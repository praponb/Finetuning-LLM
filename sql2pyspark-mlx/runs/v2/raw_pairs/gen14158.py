from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Kerry", "FedEx", "Flash"]) | F.col("order_id").isNull())
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
    .orderBy("status")
)
