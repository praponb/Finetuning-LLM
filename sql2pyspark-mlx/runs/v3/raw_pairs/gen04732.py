from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        F.rank().over(w).alias("rnk"),
        F.ntile(2).over(w2).alias("quartile"),
    )
)
