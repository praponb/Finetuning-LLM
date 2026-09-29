from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1917000, 3653000) & F.col("budget").between(1904000, 2220000))
    .select("dept_name", "budget", "location")
    .orderBy(F.col("budget").desc(), F.col("dept_name").desc())
)
