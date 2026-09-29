from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") >= 2025) | F.col("status").isin(["IN_TRANSIT", "DELIVERED"]))
    .select("status", F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"))
)
