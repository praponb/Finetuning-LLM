from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "browser", "started_at", F.rank().over(w).alias("rnk"))
)
