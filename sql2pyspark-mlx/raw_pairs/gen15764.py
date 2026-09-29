from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("new_group_flag").desc(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("new_group_flag", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("service", "attr_key").orderBy("burst_id", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last_known_new_group_flag", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("last_known_new_group_flag").desc(), "id", "attr_key")
w8 = Window.partitionBy("attr_key")
w9 = Window.partitionBy("attr_key").orderBy("last_known_new_group_flag_2", "id", "attr_key").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("attr_key").orderBy("last_known_new_group_flag_2", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("attr_key").orderBy("running_last_known_new_group_flag", "id", "attr_key")
w12 = Window.partitionBy("attr_key").orderBy(F.col("seq_no").desc(), "id", "attr_key")
w_part = Window.partitionBy("attr_key").orderBy("last_known_new_group_flag", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("attr_key").orderBy("seq_no_avg_5", "id", "attr_key")
w15 = Window.orderBy("seq_no_avg_5", "id", "attr_key")
w16 = Window.partitionBy("attr_key").orderBy("seq_no_3", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w17 = Window.partitionBy("attr_key").orderBy(F.col("next_first_known_last_known_new_group_flag_2").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

gaps = (
    flattened
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

enriched = (
    sessionized
    .filter(F.col("burst_id") >= 3)
    .select(
        "id",
        "attr_key",
        F.row_number().over(w3).alias("seq_no"),
        F.last("new_group_flag", ignorenulls=True).over(w4).alias("last_known_new_group_flag"),
        F.last("new_group_flag", ignorenulls=True).over(w5).alias("last_known_new_group_flag_2"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("last_known_new_group_flag_2").over(w6).alias("cumulative_last_known_new_group_flag_2"),
        F.row_number().over(w7).alias("seq_no_2"),
        F.sum("last_known_new_group_flag_2").over(w8).alias("attr_key_sum_last_known_new_group_flag_2"),
        F.avg("seq_no").over(w9).alias("seq_no_avg_5"),
        F.sum("last_known_new_group_flag").over(w10).alias("running_last_known_new_group_flag"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "attr_key",
        "seq_no",
        "last_known_new_group_flag",
        "cumulative_last_known_new_group_flag_2",
        "seq_no_avg_5",
        "running_last_known_new_group_flag",
        F.lag("seq_no", 2).over(w_recent).alias("prev_seq_no"),
        F.row_number().over(w12).alias("row_seq"),
        F.first("last_known_new_group_flag_2", ignorenulls=True).over(w_part).alias("first_known_last_known_new_group_flag_2"),
        F.row_number().over(w_recent).alias("seq_no_3"),
        F.lag("last_known_new_group_flag_2", 2).over(w14).alias("prev_last_known_new_group_flag_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "attr_key",
        "last_known_new_group_flag",
        "cumulative_last_known_new_group_flag_2",
        "seq_no_avg_5",
        "row_seq",
        "first_known_last_known_new_group_flag_2",
        "seq_no_3",
        "prev_last_known_new_group_flag_2",
        F.lead("first_known_last_known_new_group_flag_2", 1).over(w15).alias("next_first_known_last_known_new_group_flag_2"),
        F.sum("row_seq").over(w16).alias("cumulative_row_seq"),
        F.stddev_samp("row_seq").over(w8).alias("attr_key_stddev_samp_row_seq"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w17))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "attr_key",
        "cumulative_last_known_new_group_flag_2",
        "seq_no_avg_5",
        "first_known_last_known_new_group_flag_2",
        "seq_no_3",
        "prev_last_known_new_group_flag_2",
        "cumulative_row_seq",
        "rank_in_group",
        F.when(F.col("attr_key").rlike("^(version|trace)$"), "PRIORITY").when(F.col("attr_key").rlike("^(region)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("attr_key_zone"),
    )
    .orderBy(F.col("seq_no_3").desc(), F.col("seq_no_avg_5").desc())
)
