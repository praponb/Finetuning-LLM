from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "hire_date",
        "job_title",
        F.first("job_title").over(w).alias("first_job_title"),
    )
    .orderBy(F.col("hire_date").desc(), F.col("first_job_title").desc())
)
