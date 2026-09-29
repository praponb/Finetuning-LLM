from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "manager_id", "hire_date", "salary")
)
