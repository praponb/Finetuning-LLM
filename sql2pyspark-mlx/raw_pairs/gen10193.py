from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("started_at") >= "2023-06-01")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
)
