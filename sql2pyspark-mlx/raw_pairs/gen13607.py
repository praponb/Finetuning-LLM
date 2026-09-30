from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page").alias("web_event_page"))
)

result = (
    with_last
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "event_type",
        "web_event_page",
        F.when(F.col("country").rlike("^(TH|US)$"), "EXTENDED").when(F.col("country").rlike("^(SG|JP)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("event_type").desc_nulls_last())
)
