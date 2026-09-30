from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(
        F.col("o.channel"),
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.count(F.when(F.col("s.carrier") == "DHL", 1)).alias("dhl_count"),
    )
    .filter(F.col("dhl_count") > 2)
)
