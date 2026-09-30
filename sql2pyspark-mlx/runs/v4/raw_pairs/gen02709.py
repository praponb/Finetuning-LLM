from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") == 14) & (F.col("revenue") <= 2200))
    .select("units", "store_id", "sale_date")
)
