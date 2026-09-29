from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-01-01")
    .select(
        "cost",
        "order_id",
        F.when(F.col("cost") >= 83, "High").when(F.col("cost") >= 43, "Medium").otherwise("Low").alias("cost_band"),
        F.date_sub("delivered_date", 46).alias("delivered_date_minus_46d"),
    )
)
