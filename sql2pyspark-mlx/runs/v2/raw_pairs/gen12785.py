from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("customer_id") == 31)
    .select("id", "country", "started_at", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "country", "started_at")
    .orderBy("started_at")
)
