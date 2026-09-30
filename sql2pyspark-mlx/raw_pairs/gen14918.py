from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 5, 6]) & (F.col("store_id") == 5) & F.month("sale_date").isin([1, 2, 7]))
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)
