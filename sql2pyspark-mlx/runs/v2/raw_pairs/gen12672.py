from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary")
w2 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "dept_id",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.dense_rank().over(w2).alias("salary_dense_rank_2"),
    )
)
