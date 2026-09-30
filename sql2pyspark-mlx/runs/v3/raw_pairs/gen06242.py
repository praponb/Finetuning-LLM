from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2024-07-21")
    .select("id", "browser", "started_at", F.row_number().over(w).alias("rn"))
)
