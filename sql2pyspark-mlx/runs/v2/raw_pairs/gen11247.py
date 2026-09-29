from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")

result = df_sessions.select("id", "started_at", F.row_number().over(w).alias("row_num"))
