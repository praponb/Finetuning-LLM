from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ARO%"))
    .select("job_title", "manager_id")
    .unionByName(
        df_employees
        .filter(F.length("email") > 6)
        .select("job_title", "manager_id")
    )
)
