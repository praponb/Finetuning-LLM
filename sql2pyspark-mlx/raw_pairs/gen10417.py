from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").desc(), "id")

latest = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("started_at").alias("first_seen"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
    )
    .filter(F.col("span_minutes") >= 0)
    .orderBy(F.col("year_start").desc())
)
