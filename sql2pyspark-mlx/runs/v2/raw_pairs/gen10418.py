from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2024-05-20") & (F.col("dept_id") == 22))
    .select("id", "dept_id", F.initcap(F.trim("email")).alias("email_title"))
)
