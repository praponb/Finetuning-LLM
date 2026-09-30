from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select(
        "id",
        "carrier",
        "shipped_date",
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)
