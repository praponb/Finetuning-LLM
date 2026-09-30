from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

with_windows = (
    df_departments
    .select(
        "*",
        F.count("budget").over(w).alias("location_count_budget"),
        F.lag("budget", 2, 0).over(w2).alias("prev_budget"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        "prev_budget",
        F.when(F.col("dept_name").rlike("^(Engineering|Legal)$"), "PRIORITY").when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "EXTENDED").otherwise("CORE").alias("dept_name_zone"),
    )
    .orderBy(F.col("id").desc(), "dept_name_zone")
)
