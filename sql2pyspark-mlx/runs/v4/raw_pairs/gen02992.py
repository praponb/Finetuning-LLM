from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location")
w_recent = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .withColumn("last_known_budget", F.last("budget", ignorenulls=True).over(w))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("budget_bucket", F.ntile(10).over(w_recent))
)
