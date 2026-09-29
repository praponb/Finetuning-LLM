from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

result = (
    valid
    .select("id", "order_id")
    .intersect(
        df_shipments
        .filter(F.col("cost") > 89)
        .select("id", "order_id")
    )
)
