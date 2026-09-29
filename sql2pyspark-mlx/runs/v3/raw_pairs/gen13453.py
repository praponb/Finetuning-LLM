from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 6, 10]) & (F.col("revenue").isNull() | F.col("units").between(18, 36)))
    .select("store_id", "sale_date", "units")
)
