from pyspark.sql import functions as F

result = (
    df_products
    .agg(F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"))
)
