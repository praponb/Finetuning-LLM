from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "level").orderBy("hire_date", "id")
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "hire_date",
        "is_remote",
        "level",
        F.row_number().over(w).alias("row_seq"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
        F.rank().over(w3).alias("salary_rank"),
        F.sum("salary").over(w4).alias("cumulative_salary_2"),
    )
)
