from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("cost") > 91))
    .groupBy("carrier", F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"))
    .filter(F.col("dhl_count") < 30)
)
