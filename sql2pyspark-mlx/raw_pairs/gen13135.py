from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .groupingSets(
        [["activity_month", "country"], ["activity_month"], []],
        "activity_month",
        "country",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.grouping_id().alias("grouping_id"),
    )
)
