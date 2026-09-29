from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "hire_date",
        "manager_id",
        "level",
        "job_title",
        "salary",
        F.last("manager_id").over(w).alias("last_manager_id"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
    .orderBy(F.col("last_manager_id").desc(), F.col("dept_id").desc())
)
