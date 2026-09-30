from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_country_kr_distinct_ids_by = Window.partitionBy("n_country_kr", "distinct_ids").orderBy("n_country_not_in_us_kr", "country").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_country_kr_distinct_ids_by_2 = Window.partitionBy("n_country_kr", "distinct_ids").orderBy("n_country_not_in_us_kr", "country")
w_n_country_kr_distinct_ids_by_3 = Window.partitionBy("n_country_kr", "distinct_ids").orderBy("n_country_not_in_us_kr")

prepared = (
    df_artists
    .filter((F.col("debut_year") < 2015) & (F.col("country") != "BR"))
    .select(
        "id",
        "artist_name",
        "country",
        "is_verified",
    )
)

derived = (
    prepared
    .select(
        "*",
        F.substring(F.trim(F.col("artist_name")), 1, 4).alias("artist_name_prefix"),
        F.initcap(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
        F.concat(F.col("artist_name"), F.lit("#"), F.col("id").cast("string")).alias("artist_name_ref"),
    )
)

screened = (
    derived.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.title").isNotNull()), "left_anti")
)

qualified = (
    screened.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.is_explicit")) & (F.col("t.bpm").between(76, 156)), "left_semi")
)

qualified_2 = (
    qualified.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.size(F.col("t.moods")) >= 2) & (F.col("t.duration_sec") < 402), "left_anti")
    .filter(F.upper(F.trim(F.col("artist_name"))).like("S%"))
)

summary = (
    qualified_2
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("country") != "KR", 1)).alias("n_country_kr"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(~F.col("country").isin("TH", "US"), 1).otherwise(0)).alias("n_country_not_in_th_us"),
        F.sum(F.when(~F.col("country").isin("US", "KR"), 1).otherwise(0)).alias("n_country_not_in_us_kr"),
    )
)

windowed = (
    summary
    .select(
        "country",
        "n_country_kr",
        "n_rows",
        "distinct_ids",
        "n_country_not_in_th_us",
        "n_country_not_in_us_kr",
        F.last("distinct_ids").over(w_n_country_kr_distinct_ids_by).alias("last_distinct_ids"),
        F.ntile(5).over(w_n_country_kr_distinct_ids_by_2).alias("n_country_not_in_us_kr_quintile"),
        F.round(F.percent_rank().over(w_n_country_kr_distinct_ids_by_3), 3).alias("n_country_not_in_us_kr_percent_rank"),
        F.nth_value("distinct_ids", 2).over(w_n_country_kr_distinct_ids_by).alias("second_distinct_ids"),
        F.lead("n_country_not_in_th_us").over(w_n_country_kr_distinct_ids_by_2).alias("next_n_country_not_in_th_us"),
        F.round(F.cume_dist().over(w_n_country_kr_distinct_ids_by_2), 3).alias("n_country_not_in_us_kr_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "country",
        "n_country_not_in_us_kr_cume_dist",
        "n_country_not_in_us_kr_percent_rank",
    )
    .unpivot(
        ids=["country"],
        values=["n_country_not_in_us_kr_cume_dist", "n_country_not_in_us_kr_percent_rank"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
