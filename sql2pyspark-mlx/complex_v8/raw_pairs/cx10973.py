from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_country_len = Window.orderBy(F.col("country_len").asc_nulls_last(), "id")
w_debut_year_tier_2 = Window.partitionBy("debut_year_tier_2")
w_all_by_country_len_ties = Window.orderBy(F.col("country_len").desc())
w_all_by_country_len_moving_3 = Window.orderBy(F.col("country_len").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_country_len_prior_2 = Window.orderBy(F.col("country_len").desc(), "id").rowsBetween(-2, -1)

cleaned_artists = (
    df_artists
    .filter(~F.col("country").isin("KR", "BR"))
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        F.length(F.trim(F.col("country"))).alias("country_len"),
        F.when(F.col("debut_year") >= 2016, "large").when(F.col("debut_year") >= 1997, "medium").otherwise("small").alias("debut_year_tier"),
        F.when(F.col("debut_year") >= 2018, "large").when(F.col("debut_year") >= 1997, "medium").otherwise("small").alias("debut_year_tier_2"),
    )
)

filtered = (
    cleaned_artists
    .crossJoin(cleaned_artists.agg(F.avg("debut_year").alias("overall_avg_debut_year")))
    .filter(F.col("debut_year") > F.col("overall_avg_debut_year"))
    .drop("overall_avg_debut_year")
)

windowed = (
    filtered
    .select(
        "*",
        F.lag("country_len", 3).over(w_all_by_country_len).alias("prev3_country_len"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_debut_year_tier_2), 2).alias("debut_year_vs_debut_year_tier_2_avg"),
    )
)

ranked = (
    windowed
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        "country_len",
        "debut_year_tier",
        "debut_year_vs_debut_year_tier_2_avg",
        F.rank().over(w_all_by_country_len_ties).alias("country_len_rank"),
        F.round(F.avg("prev3_country_len").over(w_all_by_country_len_moving_3), 2).alias("prev3_country_len_avg_4"),
        F.sum("prev3_country_len").over(w_all_by_country_len_prior_2).alias("prev3_country_len_sum_2_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        F.col("country_len").alias("country_len_v"),
        F.col("prev3_country_len_sum_2_prior").alias("m_prev3_country_len_sum_2_prior"),
    )
    .unpivot(
        ids=["id"],
        values=["country_len_v", "m_prev3_country_len_sum_2_prior"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
