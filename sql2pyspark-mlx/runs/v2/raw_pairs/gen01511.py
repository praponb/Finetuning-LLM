from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("sale_date") >= "2025-10-01")

product_id_summary = (
    base
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = product_id_summary.select("product_id", "first_sale_date")
