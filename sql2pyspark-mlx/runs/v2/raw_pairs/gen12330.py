from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2023-06-01") & (F.col("shipped_date") >= "2023-08-01"))
    .select(
        "id",
        "delivered_date",
        "carrier",
        "order_id",
        F.round("cost", 0).alias("cost_rounded"),
    )
)
