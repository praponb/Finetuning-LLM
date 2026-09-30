from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "full_name",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "manager_id", "hire_date", "full_name")
    .orderBy(F.col("full_name").desc())
)
