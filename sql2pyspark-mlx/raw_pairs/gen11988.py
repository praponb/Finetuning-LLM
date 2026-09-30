from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") >= 2025) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "status",
        "shipped_date",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
    )
    .orderBy("status")
)
