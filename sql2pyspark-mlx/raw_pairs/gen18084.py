from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/checkout", "/product"]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
)
