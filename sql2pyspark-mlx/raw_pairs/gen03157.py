from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-03-01")
    .select(
        "sale_date",
        "revenue",
        F.when(F.col("units") >= 38, "High").when(F.col("units") >= 9, "Medium").otherwise("Low").alias("units_band"),
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
)
