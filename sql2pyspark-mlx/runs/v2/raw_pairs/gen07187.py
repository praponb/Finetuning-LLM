from pyspark.sql import functions as F

min_revenue = df_store_sales.filter(F.col("sale_date") > "2024-04-08").agg(F.min("revenue")).first()[0]

result = (
    df_store_sales
    .filter(F.col("revenue") == min_revenue)
    .select("id", "revenue", "store_id")
)
