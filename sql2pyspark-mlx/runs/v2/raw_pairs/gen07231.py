from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date")
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "hire_date",
        "manager_id",
        F.dense_rank().over(w).alias("hire_date_dense_rank"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("rn").desc())
)
