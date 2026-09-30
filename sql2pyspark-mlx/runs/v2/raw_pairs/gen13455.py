from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Analyst", "Engineer", "Intern"]))
    .select("id", "full_name")
    .intersect(
        df_employees
        .filter(F.col("hire_date") < "2025-10-01")
        .select("id", "full_name")
    )
    .orderBy("full_name")
)
