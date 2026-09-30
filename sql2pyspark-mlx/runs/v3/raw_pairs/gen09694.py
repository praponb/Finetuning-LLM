from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L1")
    .select(
        "manager_id",
        F.date_format("hire_date", "yyyy-MM-dd").alias("hire_date_label"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)
