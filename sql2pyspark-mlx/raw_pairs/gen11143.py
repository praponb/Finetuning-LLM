from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level", "dept_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.row_number().over(w).alias("row_seq"),
        F.percent_rank().over(w2).alias("salary_percent_rank"),
        F.sum("salary").over(w3).alias("cumulative_salary"),
    )
)
