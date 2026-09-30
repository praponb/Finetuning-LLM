from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.date_add("shipped_date", 84).alias("shipped_date_plus_84d"),
        F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"),
    )
)

result = (
    cleaned
    .select("order_id", "id")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id") == 5)
        .select("order_id", "id")
    )
)
