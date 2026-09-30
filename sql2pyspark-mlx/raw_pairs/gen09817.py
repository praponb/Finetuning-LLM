from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select(
        "id",
        "dept_name",
        "budget",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.ntile(3).over(w2).alias("quartile"),
    )
)
