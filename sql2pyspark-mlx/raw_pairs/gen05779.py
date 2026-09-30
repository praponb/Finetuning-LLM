from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([8, 33]))
    .select(
        "id",
        "dept_id",
        "salary",
        F.max("salary").over(w).alias("dept_id_max_salary"),
    )
    .orderBy("dept_id")
)
