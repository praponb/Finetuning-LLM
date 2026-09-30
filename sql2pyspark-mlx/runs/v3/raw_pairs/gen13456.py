from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([5, 6, 10]))
    .agg(
        F.min("revenue").alias("min_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)
