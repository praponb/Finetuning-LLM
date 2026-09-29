from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 72) & (F.col("carrier") == "Flash"))
    .select(
        "id",
        "delivered_date",
        "order_id",
        "status",
        F.dayofmonth("shipped_date").alias("shipped_date_day"),
    )
    .orderBy(F.col("status").desc())
)
