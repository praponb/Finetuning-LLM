from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name", "location")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 7)
    .select("dept_name", "location", "row_count")
)
