from pyspark.sql import functions as F

result = (
    df_employees
    .select("job_title", "level")
    .subtract(
        df_employees
        .filter(F.col("salary") > 66000)
        .select("job_title", "level")
    )
)
