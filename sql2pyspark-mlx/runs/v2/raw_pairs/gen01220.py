from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select("id", "customer_id", "started_at", F.row_number().over(w).alias("row_num"))
)
