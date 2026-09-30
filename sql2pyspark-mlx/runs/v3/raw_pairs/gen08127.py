from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.lead("budget", 2).over(w).alias("next_budget"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy(F.col("next_budget").desc())
)
