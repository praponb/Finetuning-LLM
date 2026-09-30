from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([6, 13, 16, 33]))
    .select("id", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("salary").between(113000, 127000))
        .select("id", "hire_date")
    )
)
