from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last(), "id")
w4 = Window.orderBy(F.col("rating").asc_nulls_last())
w5 = Window.partitionBy("country").orderBy("rating")
w6 = Window.orderBy("is_singapore", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city", "country").orderBy("latest_rank")

deduped = (
    df_suppliers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        "latest_rank",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank_2", F.row_number().over(w3))
    .filter(F.col("latest_rank_2") == 1)
)

valid = (
    ranked
    .filter(F.col("city").isNotNull() & (F.col("rating") < 3.5))
    .select(
        "id",
        "country",
        "city",
        "rating",
        "latest_rank",
        "is_singapore",
        "latest_rank_2",
        F.least("latest_rank_2", "is_singapore").alias("least_latest_rank_2_is_singapore"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.dense_rank().over(w4).alias("rating_dense_rank"),
        F.cume_dist().over(w5).alias("rating_cume_dist"),
        F.last("rating", ignorenulls=True).over(w6).alias("last_known_rating"),
    )
)

deduped_suppliers = (
    with_windows
    .withColumn("latest_rank_3", F.rank().over(w7))
    .filter(F.col("latest_rank_3") == 1)
    .drop("latest_rank_3")
)

result = (
    deduped_suppliers
    .groupBy(
        F.when(F.col("least_latest_rank_2_is_singapore") >= 80, "LARGE").otherwise("SMALL").alias("least_latest_rank_2_is_singapore_tier"),
        F.when(F.col("last_known_rating") >= 3.5, "LARGE").otherwise("SMALL").alias("last_known_rating_tier"),
    )
    .agg(
        F.percentile_approx("last_known_rating", 0.5).alias("last_known_rating_p50"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.avg(F.when(F.col("country") == "JP", F.col("rating_cume_dist"))).alias("jp_avg_rating_cume_dist"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("least_latest_rank_2_is_singapore_tier").desc_nulls_first())
)
