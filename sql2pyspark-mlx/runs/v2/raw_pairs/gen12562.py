from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "status",
        "order_id",
        F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0).alias("is_in_transit"),
        F.col("cost").cast("int").alias("cost_int"),
    )
)
