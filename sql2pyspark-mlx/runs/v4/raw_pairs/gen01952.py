from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "salary",
        "hire_date",
        "full_name",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
    .orderBy("email_user", "hire_date")
)
