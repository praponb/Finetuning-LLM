from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 17)
    .select("units", F.least("units", "revenue").alias("least_units_revenue"))
    .orderBy("least_units_revenue")
)
