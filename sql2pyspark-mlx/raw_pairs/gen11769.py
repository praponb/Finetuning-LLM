from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")

result = df_departments.select("id", "budget", F.row_number().over(w).alias("row_num"))
