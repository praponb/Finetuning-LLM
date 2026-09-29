from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.orderBy("last4_sum_rating", "id")
w4 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH", "JP"]))
    .select("*", F.concat("country", F.lit(", "), "city").alias("country_city"))
)

with_windows = (
    filtered
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "city",
        "rating",
        "country_city",
        F.min("rating").over(w).alias("last4_min_rating"),
        F.sum("rating").over(w).alias("last4_sum_rating"),
        F.avg("rating").over(w).alias("last4_avg_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)

with_windows_suppliers = (
    with_windows
    .select(
        "id",
        "last4_sum_rating",
        "last4_avg_rating",
        F.ntile(10).over(w3).alias("last4_sum_rating_bucket"),
        F.last("city", ignorenulls=True).over(w4).alias("last_known_city"),
    )
)

eligible = (
    with_windows_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") < 380), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price").isNotNull()), "left_anti")
)

stats = (
    kept
    .withColumn("last4_sum_rating_tier", F.when(F.col("last4_sum_rating") >= 18.4, "LARGE").otherwise("SMALL"))
    .withColumn("last4_avg_rating_tier", F.when(F.col("last4_avg_rating") >= 3.6, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["last4_sum_rating_tier", "last4_avg_rating_tier"], ["last4_sum_rating_tier"]],
        "last4_sum_rating_tier",
        "last4_avg_rating_tier",
    )
    .agg(
        F.min("last4_sum_rating").alias("min_last4_sum_rating"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last4_avg_rating", 0.5).alias("last4_avg_rating_p50"),
    )
)

result = (
    stats
    .select(
        "last4_sum_rating_tier",
        "min_last4_sum_rating",
        "row_count",
        "distinct_ids",
        "last4_avg_rating_p50",
    )
)
