from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "level",
        "hire_date",
        F.count("*").over(w).alias("manager_id_count"),
        F.first("manager_id").over(w2).alias("first_manager_id"),
    )
)
