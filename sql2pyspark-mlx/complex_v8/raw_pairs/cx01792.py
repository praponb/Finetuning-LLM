from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_by_debut_year_fu = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_is_verified = Window.partitionBy("country", "is_verified")
w_country_is_verified_by_debut = Window.partitionBy("country", "is_verified").orderBy(F.col("debut_year_distance").desc(), "id")
w_is_verified_country_by_debut = Window.partitionBy("is_verified", "country").orderBy("debut_year", "id").rowsBetween(-6, -1)
w_country_is_verified_by_debut_2 = Window.partitionBy("country", "is_verified").orderBy("debut_year", "id")
w_all_by_debut_year_ties = Window.orderBy(F.col("debut_year").desc())
w_all_by_debut_year_full = Window.orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_is_verified_by_debut_3 = Window.partitionBy("country", "is_verified").orderBy(F.col("debut_year_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_by_debut_year = Window.partitionBy("country").orderBy(F.col("debut_year").desc(), "id")
w_is_verified = Window.partitionBy("is_verified")
w_all_by_least_debut_year_chan = Window.orderBy("least_debut_year_change_debut_year_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_least_debut_year_chan_2 = Window.orderBy("least_debut_year_change_debut_year_avg_6_prior", "id")
w_is_verified_seq_no_by_debut = Window.partitionBy("is_verified", "seq_no").orderBy("debut_year_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_by_debut_year_av = Window.partitionBy("is_verified").orderBy("debut_year_avg_6_prior", "id")

cleaned_artists = (
    df_artists
    .filter((F.col("debut_year") <= 2016) | (~F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.upper(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
        F.abs(F.col("debut_year") - 1997).alias("debut_year_distance"),
    )
)

sequenced = (
    cleaned_artists
    .select(
        "id",
        "artist_name",
        "is_verified",
        "debut_year_distance",
        F.nth_value("debut_year", 2).over(w_is_verified_by_debut_year_fu).alias("second_debut_year"),
        F.sum("debut_year_distance").over(w_country_is_verified).alias("country_is_verified_sum_debut_year_distance"),
        (F.col("debut_year_distance") - F.lag("debut_year_distance").over(w_country_is_verified_by_debut)).alias("debut_year_distance_change"),
        F.round(F.avg("debut_year").over(w_is_verified_country_by_debut), 2).alias("debut_year_avg_6_prior"),
        F.row_number().over(w_country_is_verified_by_debut_2).alias("seq_no"),
        F.round(F.cume_dist().over(w_all_by_debut_year_ties), 3).alias("debut_year_cume_dist"),
        F.nth_value("debut_year_distance", 2).over(w_all_by_debut_year_full).alias("second_debut_year_distance"),
        F.round(F.cume_dist().over(w_country_is_verified_by_debut), 3).alias("debut_year_distance_cume_dist"),
        F.sum("debut_year_distance").over(w_country_is_verified_by_debut_3).alias("running_debut_year_distance"),
        F.nth_value("debut_year_distance", 2).over(w_is_verified_by_debut_year_fu).alias("second_debut_year_distance_2"),
        (F.col("debut_year") - F.lag("debut_year").over(w_country_by_debut_year)).alias("debut_year_change"),
    )
)

enriched = (
    sequenced
    .filter(F.col("is_verified"))
    .withColumn("least_debut_year_change_debut_year_avg_6_prior", F.least(F.col("debut_year_change"), F.col("debut_year_avg_6_prior")))
)

with_windows = (
    enriched
    .select(
        "id",
        "debut_year_distance",
        "second_debut_year",
        "country_is_verified_sum_debut_year_distance",
        "seq_no",
        "second_debut_year_distance",
        "debut_year_distance_cume_dist",
        "running_debut_year_distance",
        "least_debut_year_change_debut_year_avg_6_prior",
        F.round(F.col("debut_year_distance") - F.avg("debut_year_distance").over(w_is_verified), 2).alias("debut_year_distance_vs_is_verified_avg"),
        F.sum("second_debut_year_distance_2").over(w_all_by_least_debut_year_chan).alias("running_second_debut_year_distance_2"),
        F.dense_rank().over(w_all_by_least_debut_year_chan_2).alias("least_debut_year_change_debut_year_avg_6_prior_d"),
        F.round(F.col("second_debut_year_distance") / F.nullif(F.sum("second_debut_year_distance").over(w_is_verified), F.lit(0)), 4).alias("second_debut_year_distance_share_of_is_verified"),
        F.last("debut_year_cume_dist", ignorenulls=True).over(w_all_by_least_debut_year_chan).alias("last_known_debut_year_cume_dist"),
        F.first("running_debut_year_distance").over(w_is_verified_seq_no_by_debut).alias("first_running_debut_year_distance"),
        F.lag("debut_year_avg_6_prior").over(w_is_verified_by_debut_year_av).alias("prev_debut_year_avg_6_prior"),
        F.round(F.col("debut_year_avg_6_prior") - F.avg("debut_year_avg_6_prior").over(w_is_verified), 2).alias("debut_year_avg_6_prior_vs_is_verified_avg"),
    )
)

result = (
    with_windows
    .filter(F.col("country_is_verified_sum_debut_year_distance") > 8092)
    .select(
        "id",
        "debut_year_distance",
        "second_debut_year",
        "country_is_verified_sum_debut_year_distance",
        "second_debut_year_distance",
        "debut_year_distance_cume_dist",
        "running_debut_year_distance",
        "least_debut_year_change_debut_year_avg_6_prior",
        "debut_year_distance_vs_is_verified_avg",
        "least_debut_year_change_debut_year_avg_6_prior_d",
        "debut_year_avg_6_prior_vs_is_verified_avg",
        F.round(F.col("second_debut_year") / F.nullif(F.col("debut_year_distance"), F.lit(0)), 3).alias("second_debut_year_per_debut_year_distance"),
    )
    .orderBy(F.col("country_is_verified_sum_debut_year_distance").desc(), "id")
    .limit(20)
)
