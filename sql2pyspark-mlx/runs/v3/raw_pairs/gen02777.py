from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("budget").between(1466000, 2476000))
    .select("id", "location", "budget", F.sum("budget").over(w).alias("running_budget"))
)
