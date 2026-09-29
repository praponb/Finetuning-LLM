from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("budget") < 1152000)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "dept_name", "budget", "location")
)
