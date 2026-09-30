from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 62)
    .select(
        "id",
        "carrier",
        "status",
        "order_id",
        F.col("cost").cast("int").alias("cost_int"),
    )
)
