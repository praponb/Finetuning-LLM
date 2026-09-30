from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2300, 3600))
    .orderBy(F.col("store_id").desc())
)
