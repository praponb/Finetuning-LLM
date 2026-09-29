from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date")
w2 = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "dept_id",
        "salary",
        F.rank().over(w).alias("hire_date_rank"),
        F.round(F.avg("salary").over(w2), 2).alias("salary_moving_avg_3"),
    )
)
