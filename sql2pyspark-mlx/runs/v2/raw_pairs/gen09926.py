from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .groupBy("location")
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
    .filter(F.col("unique_dept_names") < 6)
    .select("location", "unique_dept_names")
    .orderBy("unique_dept_names")
)
