from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "is_remote",
        "level",
        F.max("salary").over(w).alias("last3_max_salary"),
        F.sum("salary").over(w).alias("last3_sum_salary"),
        F.min("salary").over(w).alias("last3_min_salary"),
        F.avg("salary").over(w).alias("last3_avg_salary"),
        F.lag("salary", 2, 0).over(w2).alias("prev_salary"),
    )
)
