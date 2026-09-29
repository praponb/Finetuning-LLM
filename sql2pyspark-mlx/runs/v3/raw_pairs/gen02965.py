from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "status",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
    .orderBy("delivered_date_month_end", "id")
)
