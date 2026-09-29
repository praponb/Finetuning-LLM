from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull() & F.col("budget").isNotNull())
    .select("dept_name")
)
