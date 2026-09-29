from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
    .filter(F.col("unique_dept_names") >= 4)
    .select("location", "unique_dept_names")
)
