from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") != "Intern") & F.col("full_name").isNotNull())
)
