from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")

result = (
    df_departments
    .filter(F.col("budget") > 2676000)
    .select(
        "id",
        "dept_name",
        "budget",
        F.min("budget").over(w).alias("dept_name_min_budget"),
    )
    .orderBy(F.col("dept_name_min_budget").desc(), F.col("id").desc())
)
