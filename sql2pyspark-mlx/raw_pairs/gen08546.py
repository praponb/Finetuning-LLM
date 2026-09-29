from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("started_at")).orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w2))
    .withColumn("last_known_country_2", F.last("country", ignorenulls=True).over(w3))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.max("started_at").alias("ended_at"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .filter(F.col("firefox_rows") > 1)
    .orderBy(F.col("num_countries").desc_nulls_first())
)
