from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 5, 9]) & (F.col("store_id").isin([1, 2, 3, 5]) | (F.col("revenue") < 2200)))
)

summary = (
    recent_store_sales
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    summary
    .filter(F.col("units_range") >= 40)
    .select("store_id", "min_sale_date", "units_range")
    .orderBy("store_id", F.col("units_range").desc())
    .limit(10)
)
