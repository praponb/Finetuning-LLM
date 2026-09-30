from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-12-01")
    .select(
        "id",
        "order_id",
        "delivered_date",
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
    )
    .orderBy(F.col("delivered_date").desc(), "shipped_date_label", "id")
    .limit(5)
)
