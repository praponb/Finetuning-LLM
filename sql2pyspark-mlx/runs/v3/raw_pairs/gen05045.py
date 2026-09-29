from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Stark") & (F.col("product_name").like("Ru%") | F.col("supplier_id").isin([10, 38])))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.sum("cost").alias("total_cost"))
)
