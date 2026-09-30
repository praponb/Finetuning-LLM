from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ARO%"))
    .select("job_title")
    .distinct()
)
