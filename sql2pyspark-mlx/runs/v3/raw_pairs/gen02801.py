from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "device", "started_at", "customer_id")
)
