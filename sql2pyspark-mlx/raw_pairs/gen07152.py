from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([4, 8, 12]) | F.month("shipped_date").isin([2, 6, 11]))
    .select(
        "id",
        "carrier",
        "order_id",
        F.when(F.col("cost") >= 100, "High").when(F.col("cost") >= 21, "Medium").otherwise("Low").alias("cost_band"),
        F.round((F.datediff("delivered_date", "shipped_date") / 30), 1).alias("months_shipped_date_to_delivered_date"),
    )
)
