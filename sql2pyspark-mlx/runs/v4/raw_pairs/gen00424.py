from pyspark.sql import functions as F

max_hire_date = df_employees.filter(~F.col("job_title").isin(["Designer", "Manager", "Director"])).agg(F.max("hire_date")).first()[0]

result = (
    df_employees
    .filter(F.col("hire_date") >= max_hire_date)
    .select("id", "hire_date", "email", "is_remote")
    .orderBy("id", "email")
)
