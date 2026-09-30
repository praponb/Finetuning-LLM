from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([5, 6]) & (F.col("units") > 31))
    .select(
        "id",
        "store_id",
        F.when(F.col("units") >= 33, "High").when(F.col("units") >= 18, "Medium").otherwise("Low").alias("units_tier"),
        F.least("revenue", "units").alias("least_revenue_units"),
    )
    .orderBy(F.col("least_revenue_units").desc(), "id")
    .limit(10)
)
