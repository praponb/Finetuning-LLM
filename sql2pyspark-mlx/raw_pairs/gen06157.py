from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.year("event_time").alias("event_year"),
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)
