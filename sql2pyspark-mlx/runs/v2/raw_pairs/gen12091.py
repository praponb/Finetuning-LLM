from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "carrier",
        "cost",
        "order_id",
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
    )
    .orderBy(F.col("order_id").desc(), F.col("cost").desc())
)
