from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("salary") >= 96000)
    .select(
        "id",
        "manager_id",
        "level",
        "salary",
        F.row_number().over(w).alias("salary_row"),
    )
)
