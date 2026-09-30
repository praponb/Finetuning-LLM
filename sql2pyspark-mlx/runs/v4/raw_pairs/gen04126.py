from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.orderBy("budget", "id")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .withColumn("prev_budget", F.lag("budget", 2, 0).over(w))
    .withColumn("next_budget", F.lead("budget", 1).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
