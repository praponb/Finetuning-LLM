from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 148000)
    .select("id", "level", "hire_date")
    .intersect(
        df_employees
        .filter(F.col("dept_id") == 18)
        .select("id", "level", "hire_date")
    )
    .orderBy(F.col("hire_date").desc())
)
