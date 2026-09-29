from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title", "dept_id").orderBy("salary")
w3 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy(F.month("hire_date")).orderBy(F.col("salary").desc())
w5 = Window.partitionBy(F.date_trunc("week", "hire_date"), "level").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.rank().over(w2).alias("salary_rank"),
        F.row_number().over(w3).alias("row_seq"),
        F.dense_rank().over(w4).alias("salary_dense_rank"),
        F.avg("salary").over(w5).alias("salary_avg_3"),
    )
)
