from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([6, 8, 11]))
    .select("revenue")
    .subtract(df_products.select("price"))
)
