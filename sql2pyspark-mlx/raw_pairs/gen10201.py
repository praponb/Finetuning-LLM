from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "level",
        "email",
        F.date_sub("hire_date", 46).alias("hire_date_minus_46d"),
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
    )
)
