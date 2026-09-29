from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(135, 363) & F.col("page").isin(["/search", "/home", "/checkout"]))
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("home_count") < 53)
    .orderBy(F.col("home_count").desc())
)
