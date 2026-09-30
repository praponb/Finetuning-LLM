from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(F.count("*").alias("row_count"))
    .orderBy("row_count")
)
