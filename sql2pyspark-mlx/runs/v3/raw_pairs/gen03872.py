from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 6]) & F.month("sale_date").isin([5, 9, 10]))
    .orderBy("sale_date", F.col("store_id").desc())
)
