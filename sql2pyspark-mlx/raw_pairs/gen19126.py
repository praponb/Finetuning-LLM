from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-03-01").cast("date"))
    .withColumn("carrier_status", F.concat("carrier", F.lit(", "), "status"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("OTHER")))
)

result = (
    filtered
    .groupBy("order_id")
    .pivot("status", ["IN_TRANSIT", "DELIVERED", "LOST"])
    .agg(F.avg("cost"))
)
