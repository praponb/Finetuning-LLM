from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_label_by_n_count = Window.partitionBy("is_verified_label").orderBy(F.col("n_country_jp").desc(), "country", "genre", "is_verified_label").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_country_jp = Window.partitionBy("n_country_jp")
w_is_verified_label_by_n_count_2 = Window.partitionBy("is_verified_label").orderBy(F.col("n_country_jp").desc())
w_is_verified_label_genre_by_t = Window.partitionBy("is_verified_label", "genre").orderBy(F.col("total_debut_year_distance").desc(), "country", "genre", "is_verified_label").rowsBetween(-3, -1)

prepared = (
    df_artists
    .filter((F.col("is_verified")) & (F.col("debut_year") > 1997))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
        F.abs(F.col("debut_year") - 1997).alias("debut_year_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
)

is_verified_label_stats = (
    prepared
    .groupBy("is_verified_label")
    .agg(F.max("debut_year_distance").alias("max_debut_year_distance"))
)

above_average = (
    prepared.alias("s")
    .join(is_verified_label_stats.alias("g"), F.col("g.is_verified_label") == F.col("s.is_verified_label"), "inner")
    .filter(F.col("s.debut_year_distance") == F.col("g.max_debut_year_distance"))
    .select(
        "s.*",
        "g.max_debut_year_distance",
        F.round(F.col("s.debut_year_distance") - F.col("g.max_debut_year_distance"), 2).alias("debut_year_distance_minus_max"),
    )
)

per_group = (
    above_average
    .groupBy("country", "genre", "is_verified_label")
    .agg(
        F.sum(F.when(F.col("country").isNotNull(), 1).otherwise(0)).alias("n_country_is_not_null"),
        F.sum(F.when(F.col("country") != "JP", 1).otherwise(0)).alias("n_country_jp"),
        F.sum("debut_year_distance").alias("total_debut_year_distance"),
        F.avg("max_debut_year_distance").alias("avg_max_debut_year_distance_check"),
    )
    .filter(F.col("avg_max_debut_year_distance_check") > 303.4)
    .drop("avg_max_debut_year_distance_check")
)

tagged = (
    per_group
    .select(
        "country",
        "genre",
        "is_verified_label",
        "n_country_is_not_null",
        "n_country_jp",
        "total_debut_year_distance",
        F.when(F.col("country").isin("US", "JP"), "group_a").when(F.col("country") == "TH", "group_b").otherwise("other").alias("country_group"),
    )
)

annotated = (
    tagged
    .select(
        "country",
        "genre",
        "is_verified_label",
        "n_country_is_not_null",
        "n_country_jp",
        "total_debut_year_distance",
        "country_group",
        F.upper(F.trim(F.col("country_group"))).alias("country_group_clean"),
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
        F.round(F.col("n_country_jp") / F.nullif(F.col("n_country_is_not_null"), F.lit(0)), 3).alias("n_country_jp_per_n_country_is_not_null"),
    )
)

with_windows = (
    annotated
    .select(
        "country",
        "genre",
        "is_verified_label",
        "n_country_is_not_null",
        "total_debut_year_distance",
        "country_group_clean",
        F.last("total_debut_year_distance", ignorenulls=True).over(w_is_verified_label_by_n_count).alias("last_known_total_debut_year_distance"),
        F.last("n_country_is_not_null", ignorenulls=True).over(w_is_verified_label_by_n_count).alias("last_known_n_country_is_not_null"),
        F.min("total_debut_year_distance").over(w_n_country_jp).alias("n_country_jp_min_total_debut_year_distance"),
        F.dense_rank().over(w_is_verified_label_by_n_count_2).alias("n_country_jp_dense_rank"),
        F.sum("n_country_jp").over(w_is_verified_label_genre_by_t).alias("n_country_jp_sum_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "country",
        "genre",
        "n_country_jp_min_total_debut_year_distance",
        "n_country_is_not_null",
    )
    .unpivot(
        ids=["country", "genre"],
        values=["n_country_jp_min_total_debut_year_distance", "n_country_is_not_null"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
