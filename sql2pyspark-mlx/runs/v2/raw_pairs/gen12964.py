from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("dept_name")
    .intersect(df_departments.filter(F.col("budget") > 1752000).select("dept_name"))
)
