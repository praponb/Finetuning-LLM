from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        "full_name",
        F.first("full_name").over(w).alias("first_full_name"),
    )
    .orderBy(F.col("id").desc(), F.col("full_name").desc())
)
