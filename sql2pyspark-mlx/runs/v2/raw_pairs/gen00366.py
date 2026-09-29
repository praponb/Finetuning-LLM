from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["Flash", "DHL"]))
    .select(
        "shipped_date",
        "carrier",
        F.add_months("delivered_date", 12).alias("delivered_date_plus_months"),
        F.concat("carrier", F.lit(", "), "status").alias("carrier_status"),
    )
)
