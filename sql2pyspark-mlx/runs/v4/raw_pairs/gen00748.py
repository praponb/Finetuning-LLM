from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country").orderBy("rating_share", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("country").orderBy("rating_percent_rank", "id")
w5 = Window.partitionBy("country").orderBy(F.col("rating_share").desc_nulls_last())

scored = (
    df_suppliers
    .select(
        "*",
        F.percent_rank().over(w).alias("rating_percent_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)

cleaned = (
    scored
    .filter(F.col("country").isin(["UK", "TH", "DE"]))
    .select(
        "id",
        "country",
        "rating_percent_rank",
        "rating_share",
        (F.col("rating") * F.col("rating_percent_rank")).alias("rating_times_rating_percent_rank"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

scored_suppliers = (
    cleaned
    .select(
        "*",
        F.avg("rating_share").over(w3).alias("rating_share_avg_3_prior"),
        F.lead("rating_times_rating_percent_rank", 1, 0).over(w4).alias("next_rating_times_rating_percent_rank"),
        F.dense_rank().over(w5).alias("rating_share_dense_rank"),
    )
)

result = (
    scored_suppliers
    .select(
        "id",
        "country",
        "rating_percent_rank",
        "rating_times_rating_percent_rank",
        "next_rating_times_rating_percent_rank",
    )
    .unpivot(
        ["id", "country"],
        ["rating_percent_rank", "rating_times_rating_percent_rank", "next_rating_times_rating_percent_rank"],
        "kpi",
        "reading",
    )
)
