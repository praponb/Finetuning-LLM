from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("budget") > 2230000)
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("id").desc(), F.col("location").desc())
)
