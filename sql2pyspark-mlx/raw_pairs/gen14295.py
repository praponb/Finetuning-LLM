from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "RETURNED", "DELIVERED"]) & ~F.col("status").isin(["RETURNED", "IN_TRANSIT", "LOST"]))
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
)
