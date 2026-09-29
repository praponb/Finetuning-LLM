from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .subtract(df_employees.filter(F.col("job_title") == "Manager").select("email"))
    .orderBy("email")
)
