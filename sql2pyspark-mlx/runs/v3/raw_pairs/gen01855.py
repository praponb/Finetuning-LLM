from pyspark.sql import functions as F

min_revenue = df_store_sales.agg(F.min("revenue")).first()[0]

result = (
    df_store_sales
    .filter(F.col("revenue") < min_revenue)
    .select("id", "revenue", "product_id", "sale_date")
    .orderBy("sale_date", "id")
    .limit(50)
)
