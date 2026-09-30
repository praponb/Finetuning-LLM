from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.col("revenue") > 2400) & (F.col("product_id") == 8) & (F.col("revenue") <= 2600)))
    .select("id", "sale_date", "product_id")
)
