from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
    .orderBy(F.col("order_id").desc(), "id")
    .limit(50)
)
