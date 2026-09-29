from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 89)
    .select(
        "id",
        "shipped_date",
        "order_id",
        F.concat("carrier", F.lit(" - "), "status").alias("carrier_status"),
    )
)
