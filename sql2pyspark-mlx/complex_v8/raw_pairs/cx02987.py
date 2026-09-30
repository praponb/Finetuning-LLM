from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id_channel = Window.orderBy("requester_id_channel_in_phone_web_chat", "requester_id_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_min_csat_n_channel_not_in_ph = Window.partitionBy("min_csat", "n_channel_not_in_phone_web").orderBy(F.col("n_channel_not_in_phone_web").asc_nulls_last())
w_n_channel_not_in_phone_web_m = Window.partitionBy("n_channel_not_in_phone_web", "min_csat")
w_all_by_requester_id_channel_2 = Window.orderBy("requester_id_channel_in_phone_web_chat", "requester_id_tier")

cleaned = (
    df_tickets
    .filter(F.col("csat") >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
    )
)

rollup_base = (
    cleaned
    .filter(F.col("csat") <= 4)
    .groupBy(
        F.when(F.col("requester_id") >= 22, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
    )
    .agg(
        F.min("csat").alias("min_csat"),
        F.sum(F.when(F.col("channel").isin("phone", "web", "chat"), F.col("requester_id")).otherwise(0)).alias("requester_id_channel_in_phone_web_chat"),
        F.count(F.when(~F.col("channel").isin("phone", "web"), 1)).alias("n_channel_not_in_phone_web"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
    )
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("requester_id_channel_in_phone_web_chat").alias("overall_min_requester_id_channel_in_phone_web_ch")))
    .filter(F.col("requester_id_channel_in_phone_web_chat") > F.col("overall_min_requester_id_channel_in_phone_web_ch"))
    .drop("overall_min_requester_id_channel_in_phone_web_ch")
)

sequenced = (
    without_matches
    .select(
        "*",
        F.first("n_channel_not_in_phone_web", ignorenulls=True).over(w_all_by_requester_id_channel).alias("first_known_n_channel_not_in_phone_web"),
        F.rank().over(w_min_csat_n_channel_not_in_ph).alias("n_channel_not_in_phone_web_rank"),
        F.min("n_channel_not_in_phone_web").over(w_n_channel_not_in_phone_web_m).alias("n_channel_not_in_phone_web_min_csat_min_n_channe"),
        F.ntile(3).over(w_all_by_requester_id_channel_2).alias("requester_id_channel_in_phone_web_chat_tercile"),
        F.round(F.col("min_csat") - F.avg("min_csat").over(w_n_channel_not_in_phone_web_m), 2).alias("min_csat_vs_n_channel_not_in_phone_web_min_csat_"),
    )
)

per_label = (
    sequenced
    .select(
        "requester_id_tier",
        "min_csat",
        "requester_id_channel_in_phone_web_chat",
        "n_channel_not_in_phone_web",
        "first_known_n_channel_not_in_phone_web",
        "n_channel_not_in_phone_web_rank",
        "n_channel_not_in_phone_web_min_csat_min_n_channe",
        "requester_id_channel_in_phone_web_chat_tercile",
        "min_csat_vs_n_channel_not_in_phone_web_min_csat_",
        F.posexplode_outer("channel_list").alias("channel_list_item_pos", "channel_list_item"),
    )
)

qualified = (
    per_label
    .crossJoin(per_label.agg(F.avg("n_channel_not_in_phone_web_rank").alias("overall_avg_n_channel_not_in_phone_web_rank")))
    .filter(F.col("n_channel_not_in_phone_web_rank") > F.col("overall_avg_n_channel_not_in_phone_web_rank"))
    .drop("overall_avg_n_channel_not_in_phone_web_rank")
)

result = (
    qualified
    .select(
        "requester_id_tier",
        "min_csat",
        "n_channel_not_in_phone_web",
        "first_known_n_channel_not_in_phone_web",
        "n_channel_not_in_phone_web_min_csat_min_n_channe",
        "requester_id_channel_in_phone_web_chat_tercile",
        "min_csat_vs_n_channel_not_in_phone_web_min_csat_",
        "channel_list_item_pos",
        "channel_list_item",
        F.lower(F.regexp_replace(F.trim(F.col("channel_list_item")), " ", "_")).alias("channel_list_item_slug"),
    )
    .orderBy(F.col("first_known_n_channel_not_in_phone_web").asc_nulls_last())
)
