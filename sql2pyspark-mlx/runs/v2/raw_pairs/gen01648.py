from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2023-03-01") & (F.year("delivered_date") >= 2024))
    .select(
        "cost",
        F.when(F.col("cost") >= 87, "High").when(F.col("cost") >= 18, "Medium").otherwise("Low").alias("cost_tier"),
    )
)
