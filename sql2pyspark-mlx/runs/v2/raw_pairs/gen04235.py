from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
    )
    .orderBy(F.col("order_id").desc(), F.col("carrier_filled").desc())
)
