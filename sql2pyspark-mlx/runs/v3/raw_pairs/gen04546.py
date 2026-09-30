from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "email",
        "manager_id",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
        F.date_add("hire_date", 17).alias("hire_date_plus_17d"),
    )
)
