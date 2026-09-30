from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_departments
    .filter(F.col("dept_name") != "Finance")
    .withColumn("last7_count_budget", F.count("budget").over(w_part))
    .withColumn("last7_max_budget", F.max("budget").over(w_part))
    .withColumn("last7_avg_budget", F.avg("budget").over(w_part))
)
