from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_event_at_year_event_type_by = Window.partitionBy("event_at_year", "event_type").orderBy(F.col("cnt").desc(), "event_type", "event_at_year")
w_event_at_year_by_n_event_typ = Window.partitionBy("event_at_year").orderBy("n_event_type_reopened")
w_event_at_year_event_type_by_2 = Window.partitionBy("event_at_year", "event_type").orderBy(F.col("cnt").desc())

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("reopened", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

summary = (
    cleaned_events
    .filter(F.col("actor") == "customer")
    .groupBy("event_type", F.year("event_at").alias("event_at_year"))
    .agg(
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("n_event_type_assigned"),
        F.count(F.when(F.col("event_type") != "reopened", 1)).alias("n_event_type_reopened"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

sequenced = (
    summary
    .select(
        "*",
        F.round(F.col("cnt") - F.avg("cnt").over(w_event_type), 2).alias("cnt_vs_event_type_avg"),
        F.ntile(4).over(w_event_at_year_event_type_by).alias("cnt_quartile"),
        F.dense_rank().over(w_event_at_year_by_n_event_typ).alias("n_event_type_reopened_dense_rank"),
        F.max("cnt").over(w_event_type).alias("event_type_max_cnt"),
        F.round(F.percent_rank().over(w_event_at_year_event_type_by_2), 3).alias("cnt_percent_rank"),
        F.lead("cnt", 1, 0).over(w_event_at_year_event_type_by).alias("next_cnt"),
    )
)

result = (
    sequenced
    .select(
        "event_type",
        "n_event_type_assigned",
        "cnt",
        "cnt_vs_event_type_avg",
        "cnt_quartile",
        "n_event_type_reopened_dense_rank",
        "event_type_max_cnt",
        "cnt_percent_rank",
        "next_cnt",
        F.abs(F.col("next_cnt") - 8).alias("next_cnt_distance"),
    )
    .orderBy(F.col("cnt_percent_rank").desc(), F.col("event_type_max_cnt").desc())
)
