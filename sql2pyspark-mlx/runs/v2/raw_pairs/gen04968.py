from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "delivered_date",
        "order_id",
        F.year("delivered_date").alias("delivered_date_year"),
        F.lower("status").alias("status_lower"),
    )
    .orderBy("delivered_date")
)
