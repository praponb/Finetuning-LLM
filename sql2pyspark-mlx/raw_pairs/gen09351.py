from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "store_id",
        F.when(F.col("units") >= 31, "High").when(F.col("units") >= 19, "Medium").otherwise("Low").alias("units_level"),
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)
