from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["IN_TRANSIT", "DELIVERED"])
    .agg(F.avg("cost").alias("value_sum"))
    .select(
        "carrier",
        F.col("IN_TRANSIT").alias("in_transit"),
        F.col("DELIVERED").alias("delivered"),
    )
)
