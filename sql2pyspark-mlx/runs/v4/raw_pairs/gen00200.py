from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
)
