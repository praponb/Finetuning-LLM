from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "salary",
        "dept_id",
        "hire_date",
        F.last("dept_id").over(w).alias("last_dept_id"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)
