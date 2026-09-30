from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") > 81) & (F.col("carrier") == "FedEx"))
    .select(
        "id",
        "carrier",
        "cost",
        "status",
        F.lower("carrier").alias("carrier_lower"),
    )
)
