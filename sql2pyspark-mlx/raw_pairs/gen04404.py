from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 11)
    .select(
        "dept_id",
        "hire_date",
        "full_name",
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
    )
    .orderBy("full_name", "hire_date_label")
)
