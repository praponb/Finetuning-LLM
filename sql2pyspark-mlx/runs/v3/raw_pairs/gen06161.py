from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        F.row_number().over(w).alias("row_num"),
        F.sum("salary").over(w2).alias("manager_id_sum_salary"),
    )
    .orderBy("salary", "manager_id")
)
