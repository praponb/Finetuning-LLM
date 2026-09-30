from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .select("id", "country", "started_at", F.ntile(3).over(w).alias("started_at_ntile"))
)
