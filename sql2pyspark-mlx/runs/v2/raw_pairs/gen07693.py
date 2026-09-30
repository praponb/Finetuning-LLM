from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 6, 12]) & (F.col("sale_date") <= "2025-02-23") & (F.col("units") >= 39))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "product_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("units_range") >= 34)
    .orderBy(F.col("product_id").desc(), F.col("units_range").desc())
)
