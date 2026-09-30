from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .filter(F.col("unique_locations") > 10)
    .select("dept_name", "unique_locations")
    .orderBy(F.col("unique_locations").desc(), F.col("dept_name").desc())
)
