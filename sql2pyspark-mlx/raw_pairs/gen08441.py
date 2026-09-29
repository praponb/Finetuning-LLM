from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "shipped_date",
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
    )
    .orderBy(F.col("shipped_date_label").desc())
)
