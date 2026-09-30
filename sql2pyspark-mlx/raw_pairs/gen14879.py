from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())

deduped = (
    df_stores
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select("id", "store_name", "dedup_rank")
    .orderBy(F.col("id").desc())
    .limit(20)
)
