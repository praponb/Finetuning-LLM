from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "dept_name", "budget", "location")
    .orderBy(F.col("budget").desc())
)
