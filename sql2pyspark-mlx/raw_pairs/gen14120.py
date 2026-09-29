from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering", "Marketing"]))
    .withColumn("running_avg_budget", F.avg("budget").over(w_recent))
    .withColumn("running_count_budget", F.count("budget").over(w_recent))
    .withColumn("running_min_budget", F.min("budget").over(w_recent))
)

result = (
    metrics
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "running_avg_budget",
        "running_min_budget",
    )
)
