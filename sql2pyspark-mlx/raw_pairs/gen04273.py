from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("event_type").orderBy("event_type_length", "id")
w2 = Window.partitionBy("event_type").orderBy("next_event_type_length", "id")
w_hist = Window.partitionBy("event_type").orderBy("next_event_type_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/product"))) & (F.col("event_time") >= F.lit("2023-08-25 18:00:00").cast("timestamp")))
    .select(
        "id",
        "session_id",
        "event_type",
        F.when(F.col("page").isin(["/home", "/search"]), "Priority").otherwise("Other").alias("page_group"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

filtered = (
    valid
    .filter(F.col("session_id") == 29)
    .select(
        "id",
        "event_type",
        "page_group",
        "resolved_page",
        F.length("event_type").alias("event_type_length"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.lead("event_type_length").over(w_part).alias("next_event_type_length"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "event_type",
        "resolved_page",
        "event_type_length",
        "next_event_type_length",
        "seq_no",
        F.count("next_event_type_length").over(w_hist).alias("running_count_next_event_type_length"),
        F.min("next_event_type_length").over(w_hist).alias("running_min_next_event_type_length"),
        F.sum("next_event_type_length").over(w_hist).alias("running_sum_next_event_type_length"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_type",
        "resolved_page",
        "running_count_next_event_type_length",
        "running_min_next_event_type_length",
    )
)
