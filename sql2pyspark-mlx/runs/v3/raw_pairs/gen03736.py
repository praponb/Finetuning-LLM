from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .select(
        "manager_id",
        "hire_date",
        "email",
        F.round((F.col("salary") * 1.07), 2).alias("salary_scaled"),
    )
)
