from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")

result = df_employees.select("id", "level", F.count("*").over(w).alias("level_count"))
