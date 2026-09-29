from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level")
w2 = Window.partitionBy("dept_id", "manager_id")

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "level",
        "salary",
        "dept_id",
        "manager_id",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
        F.sum("salary").over(w2).alias("dept_id_sum_salary"),
    )
)
