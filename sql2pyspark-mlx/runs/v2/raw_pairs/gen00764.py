from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "SG") | F.col("country").isin(["UK", "SG", "JP"]))
    .groupBy("city")
    .agg(F.count("*").alias("row_count"))
)
