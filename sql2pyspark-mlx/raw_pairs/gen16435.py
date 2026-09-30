from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("started_at"), "browser").orderBy("started_at")

result = (
    df_sessions
    .filter(F.hour("started_at").between(11, 18))
    .select("id", "started_at", "browser", F.rank().over(w).alias("rnk"))
)
