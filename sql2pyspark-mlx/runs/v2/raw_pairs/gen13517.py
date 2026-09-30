from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5, 6]))
    .select("id", "sale_date", F.round("units", 0).alias("units_rounded"))
)
