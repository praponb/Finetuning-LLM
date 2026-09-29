from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("location") != "Tokyo")
    .select("id", "location", "budget", F.sum("budget").over(w).alias("running_budget"))
)
