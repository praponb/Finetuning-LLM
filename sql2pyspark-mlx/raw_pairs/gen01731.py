from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy("hire_date")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        F.rank().over(w).alias("hire_date_rank"),
        F.first("dept_id").over(w2).alias("first_dept_id"),
    )
)
