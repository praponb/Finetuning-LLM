from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2023-07-01") & F.col("order_id").isNotNull())
    .select(
        "id",
        "cost",
        "status",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
    .orderBy("status")
)
