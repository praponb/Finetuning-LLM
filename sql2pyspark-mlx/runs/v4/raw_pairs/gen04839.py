from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, -1)

metrics = (
    df_departments
    .select(
        "*",
        F.ntile(4).over(w).alias("budget_bucket"),
        F.avg("budget").over(w2).alias("budget_avg_2_prior"),
    )
)

result = (
    metrics
    .select("id", "dept_name", "location", "budget", "budget_avg_2_prior")
    .orderBy(F.col("budget").desc_nulls_first())
)
