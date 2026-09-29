from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        F.sum("salary").over(w).alias("manager_id_sum_salary"),
    )
    .orderBy(F.col("manager_id_sum_salary").desc())
)
