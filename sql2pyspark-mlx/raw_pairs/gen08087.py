from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("manager_id", "job_title").orderBy("salary")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.max("salary").over(w_grp).alias("running_max_salary"),
        F.sum("salary").over(w_grp).alias("running_sum_salary"),
        F.avg("salary").over(w_grp).alias("running_avg_salary"),
        F.percent_rank().over(w_hist).alias("salary_percent_rank"),
    )
)
