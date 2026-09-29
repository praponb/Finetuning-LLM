from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 40) & F.month("shipped_date").isin([2, 3, 6]) & F.col("cost").isNotNull())
    .select(
        "id",
        "status",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
    )
    .orderBy("id")
    .limit(10)
)
