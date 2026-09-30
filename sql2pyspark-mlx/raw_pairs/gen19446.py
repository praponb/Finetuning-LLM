from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@startup.io"))
    .select("dept_id")
)
