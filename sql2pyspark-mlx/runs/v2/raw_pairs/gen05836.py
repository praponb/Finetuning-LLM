from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "level",
        "salary",
        F.row_number().over(w).alias("row_num"),
    )
)
