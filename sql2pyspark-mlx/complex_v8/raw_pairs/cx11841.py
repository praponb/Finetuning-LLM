from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_priority_p2_by_median_chan = Window.partitionBy("n_priority_p2").orderBy("median_channel_len", "priority", "channel", "channel_len")
w_all_by_median_channel_len = Window.orderBy(F.col("median_channel_len").asc_nulls_last(), "priority", "channel", "channel_len")
w_channel_len = Window.partitionBy("channel_len")
w_median_channel_len_channel_l = Window.partitionBy("median_channel_len", "channel_len").orderBy("median_channel_len", "priority", "channel", "channel_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("requester_id") >= 21, "A").when(F.col("requester_id") >= 11, "B").otherwise("C").alias("requester_id_tier"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

stats = (
    prepared
    .groupBy("priority", "channel", "channel_len")
    .agg(
        F.sum("csat").alias("total_csat"),
        F.count(F.when(F.col("priority") == "P2", 1)).alias("n_priority_p2"),
        F.percentile_approx("channel_len", 0.5).alias("median_channel_len"),
    )
)

sequenced = (
    stats
    .select(
        "*",
        F.row_number().over(w_n_priority_p2_by_median_chan).alias("rn"),
        F.row_number().over(w_all_by_median_channel_len).alias("median_channel_len_seq"),
        F.round(F.col("total_csat") / F.nullif(F.sum("total_csat").over(w_channel_len), F.lit(0)), 4).alias("total_csat_share_of_channel_len"),
        F.size(F.collect_set("priority").over(w_channel_len)).alias("distinct_priority_in_channel_len"),
        F.last("channel_len", ignorenulls=True).over(w_median_channel_len_channel_l).alias("last_known_channel_len"),
    )
)

result = (
    sequenced
    .filter(F.col("n_priority_p2") > 2)
    .select(
        "priority",
        "channel",
        "channel_len",
        "total_csat",
        "n_priority_p2",
        "median_channel_len",
        "rn",
        "median_channel_len_seq",
        "total_csat_share_of_channel_len",
        "distinct_priority_in_channel_len",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
    .orderBy(F.col("channel_slug").desc())
)
