from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc())
w_running = Window.partitionBy("dept_id").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.rank().over(w).alias("salary_rank"),
        F.lead("salary", 1).over(w_running).alias("next_salary"),
    )
)
