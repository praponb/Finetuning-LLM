from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["is_new_session_tier", "session_id"], ["is_new_session_tier"], []],
        "is_new_session_tier",
        "session_id",
    )
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("is_new_session")).otherwise(0)).alias("product_is_new_session"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("product_is_new_session") > 0)
    .orderBy(F.col("product_is_new_session").desc())
)
