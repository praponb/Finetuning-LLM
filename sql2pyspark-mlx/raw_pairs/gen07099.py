from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("full_name").like("Mia%"))
    .select(
        "id",
        "manager_id",
        "hire_date",
        F.row_number().over(w).alias("hire_date_row"),
    )
)
