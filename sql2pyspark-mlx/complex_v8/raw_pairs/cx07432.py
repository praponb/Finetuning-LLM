from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_by_pct_genre_indie_tie = Window.partitionBy("genre").orderBy(F.col("pct_genre_indie").desc())
w_is_verified = Window.partitionBy("is_verified")
w_genre_by_pct_genre_indie = Window.partitionBy("genre").orderBy(F.col("pct_genre_indie").desc(), "is_verified", "genre")
w_genre_by_pct_genre_indie_run = Window.partitionBy("genre").orderBy(F.col("pct_genre_indie").desc(), "is_verified", "genre").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_pct_genre_indie_share = Window.orderBy(F.col("pct_genre_indie_share_of_is_verified").desc(), "is_verified", "genre")
w_all_by_pct_genre_indie_share_2 = Window.orderBy(F.col("pct_genre_indie_share_of_is_verified").desc(), "is_verified", "genre").rowsBetween(-6, Window.currentRow)
w_all_by_pct_genre_indie_share_3 = Window.orderBy(F.col("pct_genre_indie_share_of_is_verified").desc(), "is_verified", "genre").rowsBetween(-3, -1)
w_is_verified_pct_genre_indie = Window.partitionBy("is_verified", "pct_genre_indie_dense_rank")
w_pct_genre_indie_dense_rank = Window.partitionBy("pct_genre_indie_dense_rank")

cleaned = (
    df_artists
    .filter(F.col("debut_year") >= 1994)
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.substring(F.trim(F.col("genre")), 1, 3).alias("genre_prefix"),
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
        F.length(F.trim(F.col("country"))).alias("country_len"),
    )
)

stats = (
    cleaned
    .groupBy("is_verified", "genre")
    .agg(
        F.round(F.avg("debut_year"), 2).alias("avg_debut_year"),
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.round(100 * F.count(F.when(F.col("genre") == "Indie", 1)) / F.count("*"), 1).alias("pct_genre_indie"),
    )
)

metrics = (
    stats
    .select(
        "*",
        F.dense_rank().over(w_genre_by_pct_genre_indie_tie).alias("pct_genre_indie_dense_rank"),
        F.round(F.col("pct_genre_indie") / F.nullif(F.sum("pct_genre_indie").over(w_is_verified), F.lit(0)), 4).alias("pct_genre_indie_share_of_is_verified"),
        F.lead("avg_debut_year", 2).over(w_genre_by_pct_genre_indie).alias("next2_avg_debut_year"),
        F.last("pct_genre_indie", ignorenulls=True).over(w_genre_by_pct_genre_indie_run).alias("last_known_pct_genre_indie"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.rank().over(w_all_by_pct_genre_indie_share).alias("pct_genre_indie_share_of_is_verified_rank"),
        F.round(F.avg("pct_genre_indie").over(w_all_by_pct_genre_indie_share_2), 2).alias("pct_genre_indie_avg_7"),
        F.sum("pct_genre_indie_dense_rank").over(w_all_by_pct_genre_indie_share_3).alias("pct_genre_indie_dense_rank_sum_3_prior"),
        F.count(F.lit(1)).over(w_is_verified_pct_genre_indie).alias("rows_in_is_verified_pct_genre_indie_dense_rank"),
    )
)

annotated = (
    with_trends
    .select(
        "is_verified",
        "genre",
        "avg_debut_year",
        "country_list",
        "pct_genre_indie",
        "pct_genre_indie_dense_rank",
        "pct_genre_indie_share_of_is_verified",
        "next2_avg_debut_year",
        "last_known_pct_genre_indie",
        "pct_genre_indie_share_of_is_verified_rank",
        "pct_genre_indie_avg_7",
        "pct_genre_indie_dense_rank_sum_3_prior",
        "rows_in_is_verified_pct_genre_indie_dense_rank",
        F.substring(F.trim(F.col("genre")), 1, 2).alias("genre_prefix"),
    )
)

sequenced = (
    annotated
    .withColumn("pct_genre_indie_dense_rank_avg_pct_genre_indie", F.round(F.avg("pct_genre_indie").over(w_pct_genre_indie_dense_rank), 2))
    .withColumn("pct_genre_indie_dense_rank_max_pct_genre_indie", F.max("pct_genre_indie").over(w_pct_genre_indie_dense_rank))
)

derived = (
    sequenced
    .select(
        "*",
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
        F.when(F.col("genre").isin("Pop", "Jazz"), "group_a").when(F.col("genre") == "Classical", "group_b").otherwise("other").alias("genre_group"),
        F.round(F.col("avg_debut_year") * 0.9, 2).alias("avg_debut_year_adj"),
    )
)

result = (
    derived
    .groupBy("genre")
    .pivot("genre_group", ["group_b", "group_a"])
    .agg(F.count("pct_genre_indie_share_of_is_verified"))
    .orderBy("genre")
)
