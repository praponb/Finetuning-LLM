from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") <= "2023-12-01") & (F.col("units") >= 27))
    .select(
        "store_id",
        "product_id",
        "revenue",
        F.col("units").cast("double").alias("units_num"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("units_num").desc())
)
