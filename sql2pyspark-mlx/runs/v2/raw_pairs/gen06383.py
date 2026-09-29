from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/home", "/checkout"]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "session_id", "duration_sec")
)
