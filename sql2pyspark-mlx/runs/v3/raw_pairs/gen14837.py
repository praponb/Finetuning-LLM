from pyspark.sql import functions as F

min_sale_date = df_store_sales.agg(F.min("sale_date")).first()[0]

result = (
    df_store_sales
    .filter(F.col("sale_date") == min_sale_date)
    .select("id", "sale_date", "revenue")
    .orderBy(F.col("sale_date").desc(), "id")
    .limit(1)
)
