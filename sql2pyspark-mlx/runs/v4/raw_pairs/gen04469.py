from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2169000, 3175000) | F.col("dept_name").isin(["Sales", "Legal"]))
    .select("id", "budget", "location", "dept_name")
    .orderBy(F.col("dept_name").desc())
)
