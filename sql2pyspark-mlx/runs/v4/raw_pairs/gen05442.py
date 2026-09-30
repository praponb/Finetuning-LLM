from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("manager_id").orderBy("hire_date")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "hire_date",
        F.avg("salary").over(w).alias("manager_id_avg_salary"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy("manager_id_avg_salary", "hire_date")
)
