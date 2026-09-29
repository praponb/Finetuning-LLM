from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "dept_id",
        "hire_date",
        "salary",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.count("*").over(w2).alias("dept_id_count"),
    )
)
