from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_by_debut_year = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id")
w_country_is_verified = Window.partitionBy("country", "is_verified")
w_is_verified_by_debut_year_ti = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc())
w_is_verified_by_debut_year_pr = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id").rowsBetween(-1, -1)
w_is_verified_by_debut_year_ru = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified_by_debut_year_fu = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_debut_year_full = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_artists = (
    df_artists
    .filter((F.col("debut_year") < 2020) & (F.col("genre").isin("Classical", "Pop", "HipHop")))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("debut_year") >= 2016, "A").when(F.col("debut_year") >= 1996, "B").otherwise("C").alias("debut_year_tier"),
        F.concat(F.col("country"), F.lit("#"), F.col("debut_year").cast("string")).alias("country_ref"),
    )
)

windowed = (
    cleaned_artists
    .select(
        "id",
        "country",
        "debut_year_tier",
        "country_ref",
        F.round(F.percent_rank().over(w_is_verified_by_debut_year), 3).alias("debut_year_percent_rank"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_country_is_verified), 2).alias("debut_year_vs_country_is_verified_avg"),
        F.round(F.cume_dist().over(w_is_verified_by_debut_year_ti), 3).alias("debut_year_cume_dist"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_country_is_verified), F.lit(0)), 4).alias("debut_year_share_of_country_is_verified"),
        F.max("debut_year").over(w_is_verified_by_debut_year_pr).alias("debut_year_max_1_prior"),
        F.min("debut_year").over(w_is_verified_by_debut_year_ru).alias("min_debut_year_so_far"),
        F.size(F.collect_set("debut_year_tier").over(w_country_is_verified)).alias("distinct_debut_year_tier_in_country_is_verified"),
        F.lag("debut_year").over(w_is_verified_by_debut_year).alias("prev_debut_year"),
        F.first("debut_year").over(w_is_verified_by_debut_year_fu).alias("first_debut_year"),
        F.last("debut_year").over(w_is_verified_by_debut_year_fu).alias("last_debut_year"),
        F.nth_value("debut_year", 2).over(w_all_by_debut_year_full).alias("second_debut_year"),
        F.nth_value("debut_year", 2).over(w_is_verified_by_debut_year_fu).alias("second_debut_year_2"),
    )
)

result = (
    windowed
    .filter(F.col("debut_year_cume_dist") > 0.1)
    .select(
        "id",
        "country",
        "debut_year_tier",
        "country_ref",
        "debut_year_vs_country_is_verified_avg",
        "debut_year_share_of_country_is_verified",
        "min_debut_year_so_far",
        "first_debut_year",
        "last_debut_year",
        "second_debut_year",
        (F.col("last_debut_year") - F.col("second_debut_year_2")).alias("last_debut_year_minus_second_debut_year_2"),
        F.lower(F.regexp_replace(F.trim(F.col("country_ref")), " ", "_")).alias("country_ref_slug"),
    )
    .orderBy(F.col("id").desc(), "first_debut_year")
)
