from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNull())
    .select(
        "order_id",
        "status",
        F.concat("carrier", F.lit(" "), "status").alias("carrier_status"),
        F.substring("carrier", 1, 3).alias("carrier_prefix"),
    )
    .orderBy(F.col("order_id").desc())
)
