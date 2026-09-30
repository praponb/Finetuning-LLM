from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(97000, 128000))
    .select("email")
    .unionByName(
        df_employees
        .filter(F.col("job_title").isin(["Director", "Engineer", "Intern"]))
        .select("email")
    )
)
