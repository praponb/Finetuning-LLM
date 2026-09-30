from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.hour("started_at").between(11, 16))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "browser", "started_at")
    .orderBy(F.col("browser").desc(), "started_at")
)
