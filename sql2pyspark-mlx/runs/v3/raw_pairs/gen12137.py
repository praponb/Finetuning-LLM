from pyspark.sql import functions as F

result = (
    df_store_sales
    .rollup(F.year("sale_date").alias("sale_year"), "product_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.avg("revenue").alias("avg_revenue"),
        F.min("units").alias("min_units"),
    )
)
