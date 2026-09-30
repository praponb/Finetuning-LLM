from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("page").isin(["/checkout", "/home", "/product"]))

event_cohort_summary = (
    valid_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
        F.when(F.col("event_type").isin(["click", "search"]), "group_a").otherwise("other").alias("event_type_group"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
)

result = (
    event_cohort_summary
    .filter(F.col("purchase_count") < 47)
    .select("event_cohort", "event_type_group", "purchase_count", "unique_pages")
    .orderBy("event_cohort", "event_type_group")
    .limit(10)
)
