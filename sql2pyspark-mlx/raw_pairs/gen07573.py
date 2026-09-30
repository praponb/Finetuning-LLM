from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 5, 9]) & (F.col("order_id") == 7))
    .select(
        "id",
        "carrier",
        "delivered_date",
        (F.datediff(F.current_date(), "delivered_date") / 30).alias("months_since_delivered_date"),
    )
    .orderBy(F.col("delivered_date").desc())
)
