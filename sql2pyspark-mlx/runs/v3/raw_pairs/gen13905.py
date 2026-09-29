from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
