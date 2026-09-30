from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") <= 3800) & (F.col("revenue") < 1600)) | (F.col("units") == 37))
    .select("sale_date", "units")
)
