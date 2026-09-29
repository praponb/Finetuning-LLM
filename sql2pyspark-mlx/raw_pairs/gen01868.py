from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([8, 11, 12]))
    .select(
        "id",
        "shipped_date",
        "order_id",
        "status",
        F.trim("carrier").alias("clean_carrier"),
    )
)
