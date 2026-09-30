from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Finance"]) & (F.col("budget") > 3555000) & F.col("location").isin(["New York", "Berlin"]))
    .groupBy("location", "dept_name")
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .orderBy(F.col("dept_name").desc())
)
