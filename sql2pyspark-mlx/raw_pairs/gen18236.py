from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "salary",
        "manager_id",
        F.first("manager_id").over(w).alias("first_manager_id"),
    )
)
