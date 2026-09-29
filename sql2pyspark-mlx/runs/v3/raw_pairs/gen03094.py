from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select("sale_date")
    .unionByName(df_stores.select(F.col("opened_date").alias("sale_date")))
)
