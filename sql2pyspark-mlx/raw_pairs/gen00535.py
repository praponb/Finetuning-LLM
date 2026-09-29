from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("job_title")

scored = (
    df_employees
    .select(
        "id",
        "dept_id",
        "manager_id",
        "is_remote",
        F.min("salary").over(w_part).alias("last3_min_salary"),
        F.max("salary").over(w_part).alias("last3_max_salary"),
        F.sum("salary").over(w_part).alias("last3_sum_salary"),
        F.count("salary").over(w_ordered).alias("job_title_count_salary"),
    )
)

result = (
    scored
    .select(
        "id",
        "dept_id",
        "is_remote",
        "last3_min_salary",
        "last3_max_salary",
        "last3_sum_salary",
        "job_title_count_salary",
    )
    .orderBy(F.col("id").desc(), "last3_min_salary")
    .limit(10)
)
