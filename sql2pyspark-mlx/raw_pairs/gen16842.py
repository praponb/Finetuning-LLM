from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("browser") != "Chrome")
    .select(
        "id",
        "country",
        "started_at",
        "browser",
        "device",
        F.row_number().over(w).alias("row_num"),
        F.first("device").over(w2).alias("first_device"),
    )
)
