from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("level", "dept_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("level").orderBy("salary", "id")
w4 = Window.partitionBy(F.month("hire_date"), "level").orderBy("salary")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

scored = (
    df_employees
    .filter(F.month("hire_date").isin([6, 7, 10]))
    .select(
        "*",
        F.lag("salary", 1, 0).over(w).alias("prev_salary"),
        F.dense_rank().over(w2).alias("salary_dense_rank"),
        F.row_number().over(w3).alias("seq_no"),
        F.percent_rank().over(w4).alias("salary_percent_rank"),
    )
)

result = (
    scored
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "prev_salary",
        "salary_dense_rank",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("dept_id").desc_nulls_last(), "level")
)
