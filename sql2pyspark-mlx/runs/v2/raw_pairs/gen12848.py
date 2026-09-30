from pyspark.sql import functions as F

min_revenue = df_store_sales.filter(F.col("product_id").isin([6, 7, 8, 9])).agg(F.min("revenue")).first()[0]

result = (
    df_store_sales
    .filter(F.col("revenue") <= min_revenue)
    .select("id", "revenue", "store_id")
)
