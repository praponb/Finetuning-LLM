from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "browser").orderBy("started_at", "id")

latest = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .cube("browser", "device")
    .agg(
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.grouping("browser").alias("grouping_id"),
    )
    .orderBy(F.col("browser").desc_nulls_first())
)
