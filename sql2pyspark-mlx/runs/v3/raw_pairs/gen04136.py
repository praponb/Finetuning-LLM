from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("store_id").isin([2, 5]) & F.col("units").between(15, 20)))
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.count("*").alias("store_sale_count"))
    .filter(F.col("store_sale_count") < 4)
    .orderBy(F.col("sale_year").desc())
    .limit(5)
)
