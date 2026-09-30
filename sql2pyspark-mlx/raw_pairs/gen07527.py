from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([31, 32, 37, 38]) & F.col("manager_id").isin([1, 32]))
    .select(
        "id",
        "full_name",
        "hire_date",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
    .orderBy("full_name")
)
