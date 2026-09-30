from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .withColumn("running_budget", F.sum("budget").over(w))
    .withColumn("last_known_dept_name", F.last("dept_name", ignorenulls=True).over(w))
    .withColumn("budget_bucket", F.ntile(4).over(w2))
)
