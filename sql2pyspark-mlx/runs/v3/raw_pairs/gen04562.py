from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1300, 3900) & F.col("product_id").isin([1, 2, 4, 10]) & F.col("units").isNotNull())
)
