from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
    )
)
