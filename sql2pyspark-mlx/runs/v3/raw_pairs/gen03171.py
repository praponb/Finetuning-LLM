from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L5"]))
    .select("id", "dept_id")
    .unionByName(df_employees.filter(F.col("salary") <= 134000).select("id", "dept_id"))
    .distinct()
    .orderBy("dept_id")
)
