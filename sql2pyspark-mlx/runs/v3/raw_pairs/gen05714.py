from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "country",
        "started_at",
        F.lead("started_at", 1).over(w).alias("next_started_at"),
    )
    .orderBy(F.col("next_started_at").desc(), F.col("country").desc())
)
