from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.col("is_remote") & (F.col("salary") < 131000)))
    .select(
        "id",
        "full_name",
        "hire_date",
        F.regexp_replace("email", " ", "_").alias("email_slug"),
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
    .orderBy("hire_date")
)
