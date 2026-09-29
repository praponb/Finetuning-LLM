from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "order_id",
        "cost",
        F.round("cost", 1).alias("cost_rounded"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
