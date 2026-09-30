from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 1)
    .orderBy("id")
)
