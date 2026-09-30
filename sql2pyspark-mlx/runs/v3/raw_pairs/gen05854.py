from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .select("id", "location", "budget", "dept_name", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
)
