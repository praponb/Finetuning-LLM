from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "dept_id",
        "salary",
        F.count("*").over(w).alias("manager_id_count"),
        F.avg("salary").over(w2).alias("dept_id_avg_salary"),
    )
)
