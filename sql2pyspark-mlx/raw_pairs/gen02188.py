from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

result = (
    base
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("carrier").isin(["DHL", "UPS", "Flash"]))
        .select("order_id", "id")
    )
)
