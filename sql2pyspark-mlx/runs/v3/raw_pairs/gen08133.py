from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "dept_name", "budget")
    .orderBy(F.col("id").desc())
)
