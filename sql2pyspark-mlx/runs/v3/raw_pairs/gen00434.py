from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        F.lead("salary", 1, 0).over(w).alias("next_salary"),
    )
)
