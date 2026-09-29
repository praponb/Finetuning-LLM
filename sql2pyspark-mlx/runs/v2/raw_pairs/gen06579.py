from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 37)
    .groupBy("carrier", F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
)
