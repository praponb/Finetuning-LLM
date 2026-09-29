from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .withColumn("prev_budget", F.lag("budget").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)
