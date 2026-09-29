from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 1383000) & (F.col("location") == "London"))
    .rollup("dept_name", "location")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
    )
    .orderBy(F.col("unique_dept_names").desc())
)
