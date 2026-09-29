from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "is_remote",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("salary").desc(), "pos")
)
