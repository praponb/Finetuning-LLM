from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "job_title",
        "dept_id",
        F.first("job_title").over(w).alias("first_job_title"),
        F.first("job_title").over(w2).alias("first_job_title_2"),
    )
)
