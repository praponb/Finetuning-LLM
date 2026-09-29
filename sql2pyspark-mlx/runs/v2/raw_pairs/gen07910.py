from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .orderBy("browser")
)
