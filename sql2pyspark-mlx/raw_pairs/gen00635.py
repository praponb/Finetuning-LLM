from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "dept_id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([7, 27]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "level",
        F.count("salary").over(w).alias("manager_id_count_salary"),
        F.min("salary").over(w2).alias("hire_date_period_min_salary"),
    )
)
