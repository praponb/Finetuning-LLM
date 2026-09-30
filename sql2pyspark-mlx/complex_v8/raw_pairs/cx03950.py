from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_count = Window.orderBy(F.col("claim_count").asc_nulls_last(), "id")
w_last_claim_reported_at_to_da = Window.partitionBy(F.to_date("last_claim_reported_at"), "level")

filtered = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.upper(F.trim(F.col("adjuster_name"))).like("H%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("adjuster_name")), 1, 4).alias("adjuster_name_prefix"),
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("id").cast("string")).alias("adjuster_name_ref"),
    )
)

labeled = (
    filtered
    .select(
        "id",
        "adjuster_name",
        "level",
        "adjuster_name_ref",
        F.concat_ws("|", F.col("team"), F.col("adjuster_name_prefix")).alias("team_adjuster_name_prefix_label"),
        F.substring(F.trim(F.col("adjuster_name_ref")), 1, 2).alias("adjuster_name_ref_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    labeled.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.dense_rank().over(w_all_by_claim_count).alias("claim_count_dense_rank"),
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_last_claim_reported_at_to_da), F.lit(0)), 4).alias("level_share_of_last_claim_reported_at_to_date_le"),
    )
)

result = (
    windowed
    .groupingSets(
        [["claim_count", "claim_count_dense_rank"], ["claim_count"]],
        "claim_count", "claim_count_dense_rank",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
    )
)
