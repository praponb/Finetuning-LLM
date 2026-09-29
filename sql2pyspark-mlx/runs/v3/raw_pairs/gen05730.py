from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        F.max("salary").over(w).alias("dept_id_max_salary"),
    )
)
