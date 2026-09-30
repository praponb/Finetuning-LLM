from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")
w_running = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rating", "id").rowsBetween(-5, -1)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.length("supplier_name") >= 8))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.when(F.col("city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") < 470), "left_semi")
)

ranked = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "rating")
)

valid = (
    ranked
    .filter(F.col("rating").between(2.3, 3.7))
    .withColumn("rating_rounded", F.round("rating", 2))
)

enriched = (
    valid
    .select(
        "id",
        "rating",
        F.lag("rating", 1, 0).over(w2).alias("prev_rating"),
        F.sum("rating").over(w_running).alias("cumulative_rating"),
        F.avg("rating").over(w4).alias("rating_avg_5_prior"),
        F.sum("rating_rounded").over(w_running).alias("running_rating_rounded"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_rating_rounded") >= 404, "HIGH").otherwise("LOW").alias("running_rating_rounded_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_rating", 0.95).alias("cumulative_rating_p95"),
        F.percentile_approx("cumulative_rating", 0.9).alias("cumulative_rating_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.percentile_approx("prev_rating", 0.9).alias("prev_rating_p90"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("cnt").desc_nulls_last())
)
