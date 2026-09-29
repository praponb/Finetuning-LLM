from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec_adjusted", "id")
w3 = Window.orderBy("rn", "id")
w4 = Window.partitionBy("event_type").orderBy("duration_sec_adjusted", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("event_type").orderBy("next_duration_sec", "id")
w6 = Window.partitionBy("event_type")
w7 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_adjusted_avg_6_prior").desc())

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/checkout"))) & (F.col("duration_sec") < 451))
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_adjusted"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "event_type", "duration_sec", "duration_sec_adjusted")
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "event_type",
        "duration_sec",
        "duration_sec_adjusted",
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
        F.avg("duration_sec_adjusted").over(w4).alias("duration_sec_adjusted_avg_6_prior"),
    )
)

metrics = (
    enriched
    .filter(F.col("duration_sec_adjusted") > 26)
    .select(
        "id",
        "event_type",
        "duration_sec_adjusted",
        "next_duration_sec",
        F.row_number().over(w_grp).alias("event_seq"),
        F.row_number().over(w).alias("event_seq_2"),
        F.sum("duration_sec_adjusted_avg_6_prior").over(w6).alias("event_type_sum_duration_sec_adjusted_avg_6_prior"),
        F.lead("duration_sec_adjusted_avg_6_prior", 1).over(w_grp).alias("next_duration_sec_adjusted_avg_6_prior"),
        F.rank().over(w7).alias("duration_sec_adjusted_avg_6_prior_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "duration_sec_adjusted",
        "next_duration_sec",
        "event_seq",
        "event_seq_2",
        "event_type_sum_duration_sec_adjusted_avg_6_prior",
        "next_duration_sec_adjusted_avg_6_prior",
        "duration_sec_adjusted_avg_6_prior_rank",
    )
)
