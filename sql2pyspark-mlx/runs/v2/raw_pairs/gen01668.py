from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "dept_id",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "manager_id", "hire_date", "dept_id")
)
