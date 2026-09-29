from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue_filled").asc_nulls_first())
w3 = Window.orderBy(F.col("revenue_filled").asc_nulls_first())
w4 = Window.partitionBy("product_id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-27").cast("date"))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
)

kept_keys = (
    filtered
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") < 3300)
        .select("store_id", "product_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "revenue_filled",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

metrics = (
    base
    .filter(F.col("store_id") == 6)
    .select(
        "store_id",
        "product_id",
        "revenue_filled",
        F.avg("revenue_filled").over(w).alias("store_id_avg_revenue_filled"),
        F.rank().over(w2).alias("revenue_filled_rank"),
        F.percent_rank().over(w3).alias("revenue_filled_percent_rank"),
        F.avg("revenue_filled").over(w4).alias("product_id_avg_revenue_filled"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select(
        "s.product_id",
        "s.store_id_avg_revenue_filled",
        "s.revenue_filled_percent_rank",
        "s.product_id_avg_revenue_filled",
        F.col("b.price"),
    )
)

result = (
    enriched
    .withColumn("product_id_avg_revenue_filled_tier", F.when(F.col("product_id_avg_revenue_filled") >= 80, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["product_id", "product_id_avg_revenue_filled_tier"], ["product_id"]],
        "product_id",
        "product_id_avg_revenue_filled_tier",
    )
    .agg(
        F.percentile_approx("store_id_avg_revenue_filled", 0.9).alias("store_id_avg_revenue_filled_p90"),
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.percentile_approx("revenue_filled_percent_rank", 0.9).alias("revenue_filled_percent_rank_p90"),
        F.min("price").alias("min_price"),
        F.percentile_approx("product_id_avg_revenue_filled", 0.5).alias("product_id_avg_revenue_filled_p50"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("revenue_filled_percent_rank_p90") > 0)
)
