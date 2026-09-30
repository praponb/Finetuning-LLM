from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6]) | F.col("product_id").isin([1, 2, 6, 10]))
)

agg = (
    base
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.countDistinct("units").alias("unique_stores"))
)

result = agg.select("sale_month", "unique_stores")
