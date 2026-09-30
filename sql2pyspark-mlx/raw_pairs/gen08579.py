from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc_nulls_first())

deduped = (
    df_sessions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "browser")
)

result = deduped.select("id")
