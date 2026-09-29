from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "level",
        "salary",
        "manager_id",
        "hire_date",
        F.min("salary").over(w).alias("level_min_salary"),
        (F.col("salary") - F.lag("salary", 2).over(w2)).alias("salary_change"),
    )
    .orderBy("level")
)
