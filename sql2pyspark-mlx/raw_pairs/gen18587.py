from pyspark.sql import functions as F

avg_units = df_store_sales.filter(F.col("store_id").isin([1, 3, 4, 6])).agg(F.avg("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") >= avg_units)
    .select("id", "units", "sale_date", "product_id")
)
