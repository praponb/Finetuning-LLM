from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")
w2 = Window.partitionBy(F.year("hire_date"), "manager_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_employees
    .filter(F.col("salary").between(111000, 133000))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.avg("salary").over(w2).alias("salary_avg_3"),
        F.sum("salary").over(w3).alias("cumulative_salary"),
        F.avg("salary").over(w4).alias("salary_avg_3_2"),
    )
)
