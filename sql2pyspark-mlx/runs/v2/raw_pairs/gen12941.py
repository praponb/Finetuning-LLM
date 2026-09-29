from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "status",
        "shipped_date",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
