from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
    .filter(F.col("corporate_count") > 8)
)
