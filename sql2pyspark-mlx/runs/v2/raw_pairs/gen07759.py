from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .select("id", "country", "started_at", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
)
