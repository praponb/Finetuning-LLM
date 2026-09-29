from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isin([11, 13]))
    .select(
        "id",
        "delivered_date",
        "shipped_date",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)
