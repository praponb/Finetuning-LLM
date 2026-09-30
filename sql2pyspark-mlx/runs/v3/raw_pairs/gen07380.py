from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("hire_date") > "2025-07-01")
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        F.lag("salary", 2).over(w).alias("prev_salary"),
    )
    .orderBy(F.col("prev_salary").desc())
)
