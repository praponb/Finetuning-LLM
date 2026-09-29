from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year = Window.orderBy(F.col("debut_year").desc(), "id")
w_all_by_debut_year_full = Window.orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_genre = Window.partitionBy("country", "genre")
w_all_by_debut_year_ties = Window.orderBy(F.col("debut_year").desc())
w_debut_year_dense_rank_n_rows = Window.partitionBy("debut_year_dense_rank", "n_rows")
w_debut_year_dense_rank_by_n_r = Window.partitionBy("debut_year_dense_rank").orderBy(F.col("n_rows").asc_nulls_last())
w_debut_year_dense_rank_by_n_r_2 = Window.partitionBy("debut_year_dense_rank").orderBy(F.col("n_rows").asc_nulls_last(), "country", "is_verified", "debut_year_dense_rank")

prepared = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.lead("debut_year").over(w_all_by_debut_year).alias("next_debut_year"),
        F.last("debut_year").over(w_all_by_debut_year_full).alias("last_debut_year"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_country_genre), 2).alias("debut_year_vs_country_genre_avg"),
        F.dense_rank().over(w_all_by_debut_year_ties).alias("debut_year_dense_rank"),
        F.max("debut_year").over(w_country_genre).alias("country_genre_max_debut_year"),
    )
)

stats = (
    sequenced
    .filter(~F.col("is_verified"))
    .groupBy("country", "is_verified", "debut_year_dense_rank")
    .agg(
        F.countDistinct("country").alias("distinct_countrys"),
        F.round(F.avg("country_genre_max_debut_year"), 2).alias("avg_country_genre_max_debut_year"),
        F.count("*").alias("n_rows"),
        F.sum("next_debut_year").alias("total_next_debut_year"),
    )
)

ranked = (
    stats
    .select(
        "*",
        F.round(F.avg("distinct_countrys").over(w_debut_year_dense_rank_n_rows), 2).alias("debut_year_dense_rank_n_rows_avg_distinct_countr"),
        F.rank().over(w_debut_year_dense_rank_by_n_r).alias("n_rows_rank"),
        F.round(F.avg("debut_year_dense_rank").over(w_debut_year_dense_rank_n_rows), 2).alias("debut_year_dense_rank_n_rows_avg_debut_year_dens"),
        (F.col("distinct_countrys") - F.lag("distinct_countrys").over(w_debut_year_dense_rank_by_n_r_2)).alias("distinct_countrys_change"),
        F.max("n_rows").over(w_debut_year_dense_rank_n_rows).alias("debut_year_dense_rank_n_rows_max_n_rows"),
        F.round(F.avg("total_next_debut_year").over(w_debut_year_dense_rank_n_rows), 2).alias("debut_year_dense_rank_n_rows_avg_total_next_debu"),
    )
)

result = (
    ranked
    .select(
        "country",
        "is_verified",
        "debut_year_dense_rank_n_rows_avg_distinct_countr",
        "debut_year_dense_rank_n_rows_avg_debut_year_dens",
    )
    .unpivot(
        ids=["country", "is_verified"],
        values=["debut_year_dense_rank_n_rows_avg_distinct_countr", "debut_year_dense_rank_n_rows_avg_debut_year_dens"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
