from pyspark.sql import functions as F

filtered_artists = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.concat_ws(": ", F.col("country"), F.col("artist_name")).alias("country_artist_name_label"),
    )
)

summary = (
    filtered_artists
    .groupBy("genre", "is_verified_label", "is_verified_flag")
    .agg(
        F.max("debut_year").alias("max_debut_year"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

flattened = (
    summary
    .select(
        "genre",
        "is_verified_label",
        "is_verified_flag",
        "max_debut_year",
        "distinct_ids",
        "artist_name_list",
        "cnt",
        F.explode_outer("country_list").alias("country_list_item"),
    )
)

flattened_2 = (
    flattened
    .select(
        "genre",
        "is_verified_label",
        "is_verified_flag",
        "max_debut_year",
        "distinct_ids",
        "cnt",
        "country_list_item",
        F.explode("artist_name_list").alias("artist_name_list_item"),
    )
)

result = (
    flattened_2
    .select(
        "genre",
        "is_verified_label",
        "is_verified_flag",
        "max_debut_year",
        "distinct_ids",
        "country_list_item",
        "artist_name_list_item",
        F.when(F.col("genre").isin("Classical", "HipHop"), "group_a").when(F.col("genre") == "Pop", "group_b").otherwise("other").alias("genre_group"),
        (F.col("max_debut_year") + F.col("cnt")).alias("max_debut_year_plus_cnt"),
    )
    .orderBy(F.col("country_list_item").desc_nulls_first(), "artist_name_list_item")
)
