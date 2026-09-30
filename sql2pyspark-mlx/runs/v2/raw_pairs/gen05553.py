from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2024-07-06")
    .select(
        "shipped_date",
        "delivered_date",
        "carrier",
        F.when(F.col("cost") >= 107, "High").when(F.col("cost") >= 48, "Medium").otherwise("Low").alias("cost_level"),
        F.date_sub("delivered_date", 87).alias("delivered_date_minus_87d"),
    )
)
