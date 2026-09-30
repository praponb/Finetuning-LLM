from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        F.avg("salary").over(w).alias("dept_id_avg_salary"),
        F.rank().over(w2).alias("salary_rank"),
    )
)
