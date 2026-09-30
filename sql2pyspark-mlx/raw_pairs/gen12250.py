from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc_nulls_first())

latest = (
    df_sessions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "customer_id", "browser", "row_num")
    .orderBy(F.col("id").desc())
)
