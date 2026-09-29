from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "city", "country")
    .agg(F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"))
)
