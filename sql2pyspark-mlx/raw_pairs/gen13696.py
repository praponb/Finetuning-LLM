from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([4, 5, 7]))
    .select("dept_id", "email")
    .unionByName(
        df_employees
        .filter(F.col("salary") > 123000)
        .select("dept_id", "email")
    )
)
