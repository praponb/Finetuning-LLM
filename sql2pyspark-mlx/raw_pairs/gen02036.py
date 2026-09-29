from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter((F.col("location") != "Berlin") & F.col("location").isNotNull())
    .withColumn("budget_band", F.when(F.col("budget") >= 4019000, "High").when(F.col("budget") >= 1073000, "Medium").otherwise("Low"))
)

result = (
    filtered
    .withColumn("running_max_budget", F.max("budget").over(w_recent))
    .withColumn("running_sum_budget", F.sum("budget").over(w_recent))
    .withColumn("running_count_budget", F.count("budget").over(w_recent))
    .withColumn("running_avg_budget", F.avg("budget").over(w_recent))
)
