from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("city")
w_hist = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)

with_windows = (
    df_suppliers
    .select(
        "*",
        F.ntile(4).over(w_grp).alias("rating_bucket"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
        F.avg("rating").over(w_hist).alias("rating_avg_5"),
    )
)

filtered = (
    with_windows
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rating_bucket",
        "rating_share",
        "rating_avg_5",
        F.substring("country", 1, 4).alias("country_prefix"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

valid = (
    filtered
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "country",
        "rating_bucket",
        "rating_share",
        "city_clean",
        F.ceil("rating_share").alias("rating_share_whole"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean_2"),
    )
)

result = (
    valid
    .groupBy("country")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("rating_share_whole", [0.25, 0.5, 0.75]).alias("rating_share_whole_quartiles"),
        F.sum(F.when(F.col("country") == "DE", F.col("rating_share_whole")).otherwise(0)).alias("de_rating_share_whole"),
    )
    .filter(F.col("num_countries") < 1)
    .orderBy("num_countries")
)
