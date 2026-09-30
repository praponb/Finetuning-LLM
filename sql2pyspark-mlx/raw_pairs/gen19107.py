from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")
w3 = Window.orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("city")
w6 = Window.orderBy("next_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("next_rating", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("rating_share_tier").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("Lio%") & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .withColumn("country_group", F.when(F.col("country").isin(["UK", "US"]), "Group A").otherwise("Other"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 300), "left_semi")
)

metrics = (
    kept
    .filter(F.col("supplier_name").like("P%"))
    .select(
        "id",
        "supplier_name",
        F.first("city", ignorenulls=True).over(w).alias("first_known_city"),
        F.lead("rating", 2).over(w2).alias("next_rating"),
        F.row_number().over(w3).alias("seq_no"),
        F.round((F.col("rating") / F.sum("rating").over(w4)), 4).alias("rating_share"),
        F.stddev_samp("rating").over(w5).alias("city_stddev_samp_rating"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.max("next_rating").over(w6).alias("running_max_next_rating"),
        F.avg("next_rating").over(w6).alias("running_avg_next_rating"),
        F.sum("next_rating").over(w6).alias("running_sum_next_rating"),
        F.avg("rating_share").over(w7).alias("rating_share_avg_6_prior"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("rating_share") >= 0.84, "LARGE").otherwise("SMALL").alias("rating_share_tier"),
        "seq_no",
    )
    .agg(
        F.percentile_approx("city_stddev_samp_rating", 0.95).alias("city_stddev_samp_rating_p95"),
        F.percentile_approx("rating_share_avg_6_prior", [0.25, 0.5, 0.75]).alias("rating_share_avg_6_prior_quartiles"),
        F.sum("running_max_next_rating").alias("total_running_max_next_rating"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w8))
    .orderBy(F.col("city_stddev_samp_rating_p95").desc_nulls_last())
)
