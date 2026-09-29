from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("dept_id") == 28)
    .select(
        "id",
        "manager_id",
        "hire_date",
        "level",
        "salary",
        F.row_number().over(w).alias("row_num"),
        (F.col("salary") - F.lag("salary", 1).over(w2)).alias("salary_change"),
    )
)
