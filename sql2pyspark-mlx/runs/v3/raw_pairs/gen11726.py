from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering"]))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.first("dept_name").over(w).alias("first_dept_name"),
    )
)
