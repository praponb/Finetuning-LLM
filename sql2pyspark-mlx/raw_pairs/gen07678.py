from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("level")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "level",
        F.max("salary").over(w).alias("manager_id_max_salary"),
        F.max("salary").over(w2).alias("level_max_salary"),
    )
)
