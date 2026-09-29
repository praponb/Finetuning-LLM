from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1311000)
    .groupBy("dept_name", "location")
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .filter(F.col("unique_locations") > 10)
    .orderBy("dept_name", F.col("location").desc())
)
