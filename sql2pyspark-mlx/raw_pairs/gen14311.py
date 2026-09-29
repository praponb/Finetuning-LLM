from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("salary") < 137000)
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.min("salary").over(w).alias("running_min_salary"),
        F.max("salary").over(w).alias("running_max_salary"),
        F.count("salary").over(w).alias("running_count_salary"),
        F.sum("salary").over(w).alias("running_sum_salary"),
    )
)
