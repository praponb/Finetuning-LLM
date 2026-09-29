from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L1", "L4"]) & (F.col("hire_date") < "2023-07-01") & F.col("full_name").like("b%"))
    .select(
        "id",
        "dept_id",
        F.round((F.col("salary") * 1.15), 2).alias("adjusted_salary"),
    )
    .orderBy("adjusted_salary")
)
