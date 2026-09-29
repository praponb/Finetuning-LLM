from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "budget", "location")
    .orderBy(F.col("id").desc(), F.col("location").desc())
)
