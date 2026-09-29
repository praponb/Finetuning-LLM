from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        F.dense_rank().over(w).alias("hire_date_dense_rank"),
    )
)
