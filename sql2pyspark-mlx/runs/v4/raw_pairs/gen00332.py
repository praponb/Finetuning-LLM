from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.month("hire_date").isin([5, 8, 12]))
    .select(
        "id",
        "manager_id",
        "salary",
        "hire_date",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "manager_id", "salary", "hire_date")
)
