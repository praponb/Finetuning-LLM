from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("country").isin(["JP", "TH"]))
    .select("id", "customer_id", "started_at", F.rank().over(w).alias("rnk"))
)
