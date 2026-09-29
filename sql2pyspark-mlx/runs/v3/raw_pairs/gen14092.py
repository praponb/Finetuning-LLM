from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "LOST"]) | F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
    .orderBy("id")
)
