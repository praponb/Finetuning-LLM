from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 6]) & F.col("units").between(19, 24))
    .select(
        "store_id",
        F.when(F.col("revenue") >= 3200, "High").when(F.col("revenue") >= 2000, "Medium").otherwise("Low").alias("revenue_band"),
        F.round((F.col("revenue") * 100), 2).alias("revenue_adjusted"),
    )
    .orderBy(F.col("store_id").desc())
)
