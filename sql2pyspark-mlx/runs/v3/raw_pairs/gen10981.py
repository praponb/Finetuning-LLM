from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "hire_date",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "manager_id", "salary", "hire_date")
    .orderBy("id")
)
