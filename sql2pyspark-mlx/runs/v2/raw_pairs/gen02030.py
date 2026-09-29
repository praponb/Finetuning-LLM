from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "salary",
        "email",
        "level",
        F.substring("email", 1, 4).alias("email_prefix"),
    )
)
