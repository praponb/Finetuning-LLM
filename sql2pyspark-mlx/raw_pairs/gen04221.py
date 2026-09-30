from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "email",
        "dept_id",
        "hire_date",
        "is_remote",
        "level",
        F.lag("salary").over(w).alias("prev_salary"),
        F.row_number().over(w).alias("seq_no"),
    )
)
