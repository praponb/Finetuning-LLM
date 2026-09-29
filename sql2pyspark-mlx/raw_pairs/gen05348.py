from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([9, 14, 39]) & (F.year("delivered_date") == 2023))
    .select(
        "id",
        "delivered_date",
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
    )
)
