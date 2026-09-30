from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Wong%"))
    .select("email")
    .intersect(df_employees.filter(F.col("job_title") == "Engineer").select("email"))
    .orderBy(F.col("email").desc())
)
