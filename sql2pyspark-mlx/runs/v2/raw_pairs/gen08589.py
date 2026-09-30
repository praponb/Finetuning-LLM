from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        F.date_format("delivered_date", "yyyy-MM-dd").alias("delivered_date_label"),
    )
)
