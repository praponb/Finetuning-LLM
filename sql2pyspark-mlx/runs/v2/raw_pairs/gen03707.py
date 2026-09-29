from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "RETURNED", "DELIVERED"]))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("unassigned")))
)

agg = (
    recent_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    agg
    .filter(F.col("delivered_cost") < 32)
    .orderBy("cost_range")
)
