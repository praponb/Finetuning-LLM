from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@startup.io") & (F.col("level") == "L5"))
    .select(
        "email",
        F.quarter("hire_date").alias("hire_date_quarter"),
        F.round("salary", 0).alias("salary_rounded"),
    )
)
