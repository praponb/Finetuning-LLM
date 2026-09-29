from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_genre_by_debut_year = Window.partitionBy("country", "genre").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_group_genre = Window.partitionBy("country_group", "genre")
w_country_is_verified = Window.partitionBy("country", "is_verified")
w_country_group_by_debut_year = Window.partitionBy("country_group").orderBy("debut_year", "id")
w_all_by_debut_year = Window.orderBy("debut_year", "id")
w_all_by_debut_year_prior_1 = Window.orderBy("debut_year", "id").rowsBetween(-1, -1)
w_country_group_by_debut_year_2 = Window.partitionBy("country_group").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_debut_year_full = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_artists = (
    df_artists
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("country").isin("KR", "JP"), "group_a").when(F.col("country") == "BR", "group_b").otherwise("other").alias("country_group"),
    )
)

with_windows = (
    base_artists
    .select(
        "*",
        F.max("debut_year").over(w_country_genre_by_debut_year).alias("max_debut_year_so_far"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_country_group_genre), 2).alias("debut_year_vs_country_group_genre_avg"),
        F.round(F.avg("debut_year").over(w_country_is_verified), 2).alias("country_is_verified_avg_debut_year"),
        F.lag("debut_year", 1, 0).over(w_country_group_by_debut_year).alias("prev_debut_year"),
        F.round(F.cume_dist().over(w_all_by_debut_year), 3).alias("debut_year_cume_dist"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_country_group_genre), F.lit(0)), 4).alias("debut_year_share_of_country_group_genre"),
        F.round(F.avg("debut_year").over(w_all_by_debut_year_prior_1), 2).alias("debut_year_avg_1_prior"),
        F.size(F.collect_set("country_group").over(w_country_is_verified)).alias("distinct_country_group_in_country_is_verified"),
        F.nth_value("debut_year", 2).over(w_country_group_by_debut_year_2).alias("second_debut_year"),
        F.nth_value("debut_year", 2).over(w_all_by_debut_year_full).alias("second_debut_year_2"),
        F.lag("debut_year").over(w_country_group_by_debut_year).alias("prev_debut_year_2"),
        F.max("debut_year").over(w_country_is_verified).alias("country_is_verified_max_debut_year"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
        F.round(F.col("debut_year_avg_1_prior") * 0.9, 2).alias("debut_year_avg_1_prior_adj"),
        F.abs(F.col("second_debut_year_2") - 1997).alias("second_debut_year_2_distance"),
    )
)

result = (
    tagged
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        "max_debut_year_so_far",
        "country_is_verified_avg_debut_year",
        "debut_year_cume_dist",
        "debut_year_avg_1_prior",
        "second_debut_year_2",
        "prev_debut_year_2",
        "country_is_verified_max_debut_year",
        "second_debut_year_2_distance",
        F.lower(F.trim(F.col("country_group"))).alias("country_group_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
    .orderBy("prev_debut_year_2", "id")
    .limit(20)
)
