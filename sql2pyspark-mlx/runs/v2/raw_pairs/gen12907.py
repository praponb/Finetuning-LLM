from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.count("*").alias("total_count"))
)
