from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "RETURNED"]))
    .select(
        "shipped_date",
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
    )
)
