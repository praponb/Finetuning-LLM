from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2025-09-01") & F.col("order_id").isNull())
    .select(
        "id",
        "delivered_date",
        "shipped_date",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
    )
)
