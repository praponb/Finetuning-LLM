from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.month("sale_date").isin([1, 8, 12]) & (F.col("revenue").between(1200, 2600) | (F.col("revenue") <= 1200))))
    .select("units", "product_id", "store_id")
)
