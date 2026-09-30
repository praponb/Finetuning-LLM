from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 3784000) & (F.col("budget") <= 1204000))
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
    .orderBy(F.col("unique_dept_names").desc())
)
