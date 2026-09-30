from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 3000) & F.col("product_id").isin([2, 4, 9, 10]))
    .select(
        "id",
        "product_id",
        F.when(F.col("units") >= 38, "High").when(F.col("units") >= 9, "Medium").otherwise("Low").alias("units_band"),
    )
    .orderBy(F.col("units_band").desc(), F.col("product_id").desc())
)
