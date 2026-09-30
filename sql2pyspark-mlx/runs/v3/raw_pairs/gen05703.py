from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Manager"]))
    .select("email")
    .subtract(df_employees.filter(F.col("salary") <= 104000).select("email"))
    .orderBy("email")
)
