from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country", "city")
w_ordered = Window.partitionBy("city", "country").orderBy("rating_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("city").orderBy(F.col("rating_share").desc_nulls_first())

scored = (
    df_suppliers
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("next_rating", F.lead("rating", 2).over(w2))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w3)), 4))
    .withColumn("rating_share_2", F.round((F.col("rating") / F.sum("rating").over(w4)), 4))
)

with_windows = (
    scored
    .select(
        "id",
        "city",
        "rating",
        "event_seq",
        "next_rating",
        "rating_share",
        F.max("rating_share").over(w_ordered).alias("running_max_rating_share"),
        F.sum("rating_share").over(w_ordered).alias("running_sum_rating_share"),
        F.min("rating_share").over(w_ordered).alias("running_min_rating_share"),
        F.avg("rating_share").over(w_ordered).alias("running_avg_rating_share"),
    )
)

matched_keys = (
    with_windows
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(~F.col("city").isin(["New York", "Berlin"]))
        .select("id", "city")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("running_avg_rating_share") >= 0.73)
    .select(
        "id",
        "city",
        "rating",
        "event_seq",
        "next_rating",
        "rating_share",
        "running_max_rating_share",
        "running_min_rating_share",
        "running_avg_rating_share",
        F.round((F.col("running_avg_rating_share") / F.col("rating")), 2).alias("running_avg_rating_share_per_rating"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand") == "Initech"), "left_anti")
)

metrics = (
    kept
    .withColumn("city_max_rating", F.max("rating").over(w6))
    .withColumn("rating_share_percent_rank", F.percent_rank().over(w7))
)

result = (
    metrics
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating_share_percent_rank")).otherwise(0)).alias("bangkok_rating_share_percent_rank"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("bangkok_count").asc_nulls_last())
)
