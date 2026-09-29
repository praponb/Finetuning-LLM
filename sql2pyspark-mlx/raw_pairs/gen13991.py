from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id")
w_grp_2 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w4 = Window.partitionBy(F.year("sale_date")).orderBy("prev_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("sale_date"), "product_id").orderBy("prev_units", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("product_id").orderBy("sale_date")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.ntile(5).over(w_grp).alias("revenue_bucket"),
        F.count("units").over(w2).alias("product_id_count_units"),
        F.lag("units", 1).over(w_grp_2).alias("prev_units"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.sum("revenue_bucket").over(w4).alias("cumulative_revenue_bucket"),
        F.avg("revenue_bucket").over(w5).alias("revenue_bucket_avg_2_prior"),
    )
)

enriched_store_sales = (
    enriched
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("%Mat%"))
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.revenue_bucket",
        "src.product_id_count_units",
        "src.cumulative_revenue_bucket",
        "src.revenue_bucket_avg_2_prior",
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
)

deduped = (
    enriched_store_sales
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    deduped
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([4, 6]))
        .select("id", "product_id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(
        F.when(F.col("revenue_bucket_avg_2_prior") >= 8, "PREMIUM").otherwise("STANDARD").alias("revenue_bucket_avg_2_prior_tier"),
        F.date_trunc("year", "sale_date").alias("year_start"),
        F.when(F.col("cumulative_revenue_bucket") >= 34, "HIGH").otherwise("LOW").alias("cumulative_revenue_bucket_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("revenue_bucket", 0.9).alias("revenue_bucket_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("cumulative_revenue_bucket"), 2).alias("avg_cumulative_revenue_bucket"),
        F.count("*").alias("event_count"),
        F.percentile_approx("product_id_count_units", [0.25, 0.5, 0.75]).alias("product_id_count_units_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    stats
    .select(
        "revenue_bucket_avg_2_prior_tier",
        "year_start",
        "cumulative_revenue_bucket_tier",
        "cnt",
        "revenue_bucket_p90",
        "avg_cumulative_revenue_bucket",
        "event_count",
        "product_id_count_units_quartiles",
    )
    .orderBy(
        F.col("revenue_bucket_p90").asc_nulls_last(),
        F.col("cumulative_revenue_bucket_tier").desc(),
    )
)
