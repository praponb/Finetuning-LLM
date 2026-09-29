from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 88) & F.col("carrier").isNotNull())
    .select(
        "status",
        "shipped_date",
        "delivered_date",
        F.when(F.col("cost") >= 87, "High").when(F.col("cost") >= 31, "Medium").otherwise("Low").alias("cost_band"),
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
    .orderBy("shipped_date")
)
