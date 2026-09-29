from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") > "2025-07-01") & (F.col("status") != "RETURNED") & (F.col("shipped_date") > "2024-10-27"))
    .select(
        "id",
        "delivered_date",
        "cost",
        F.when(F.col("cost") >= 88, "High").when(F.col("cost") >= 35, "Medium").otherwise("Low").alias("cost_band"),
        F.round((F.col("cost") * 0.85), 2).alias("cost_adjusted"),
    )
    .orderBy("id")
)
