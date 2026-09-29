from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.when(F.col("units") >= 39, "High").when(F.col("units") >= 11, "Medium").otherwise("Low").alias("units_band"),
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
    .orderBy("sale_date", F.col("units").desc())
)
