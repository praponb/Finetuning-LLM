from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 12) & (F.col("units").between(20, 35) | (F.col("revenue") <= 3700)))
    .select("id", "sale_date", "units", "product_id")
    .orderBy(F.col("id").desc(), "sale_date")
)
