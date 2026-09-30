from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.lead("budget", 2, 0).over(w).alias("next_budget"),
        F.dense_rank().over(w2).alias("budget_dense_rank"),
    )
    .orderBy(F.col("budget_dense_rank").desc())
)
