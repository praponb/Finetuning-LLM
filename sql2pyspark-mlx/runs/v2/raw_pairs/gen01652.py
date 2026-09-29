from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Engineering", "Marketing"]))
    .select("dept_name")
    .distinct()
    .orderBy(F.col("dept_name").desc())
    .limit(10)
)
