from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 148000) & F.month("hire_date").isin([3, 4, 7]))
    .select(
        "id",
        "manager_id",
        "job_title",
        F.round("salary", 2).alias("salary_rounded"),
        F.length("email").alias("email_length"),
    )
)
