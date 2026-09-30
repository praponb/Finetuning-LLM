from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.month("sale_date").isin([2, 5, 12]))

product_id_summary = (
    recent_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.count("units").alias("units_count"),
    )
)

result = product_id_summary.filter(F.col("units_count") >= 4)
