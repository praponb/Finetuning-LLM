from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT", "DELIVERED"]) & F.col("order_id").isNull())
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
    )
)
