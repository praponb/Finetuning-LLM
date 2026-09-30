from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & F.col("location").isin(["London", "New York", "Chiang Mai"]))
    .groupBy("location")
    .agg(F.count("*").alias("row_count"))
    .orderBy("location")
)
