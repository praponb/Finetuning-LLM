from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy("started_at")
)
