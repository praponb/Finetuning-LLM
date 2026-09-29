from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("opened_date"), "region")
w_hist = Window.partitionBy("region")

with_dims = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.region"), F.col("p.opened_date"), F.col("p.store_name"))
)

kept_keys = (
    with_dims
    .select("store_id", "product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("store_id") == 1)
        .select("store_id", "product_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w))
    .withColumn("corr_revenue_units_2", F.corr("revenue", "units").over(w_hist))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w_hist))
    .withColumn("corr_units_revenue_2", F.corr("units", "revenue").over(w_hist))
)

base = (
    enriched
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-04-10"))) & (F.col("corr_revenue_units_2") < 0.6))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "region",
        "opened_date",
        "store_name",
        "corr_revenue_units",
        "corr_revenue_units_2",
        "corr_units_revenue",
        "corr_units_revenue_2",
        F.when(F.col("units") >= 45, "High").when(F.col("units") >= 13, "Medium").otherwise("Low").alias("units_tier"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.*",
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("s.opened_date").alias("store_opened_date"),
    )
)

grouped = (
    joined
    .withColumn("corr_revenue_units_tier", F.when(F.col("corr_revenue_units") >= 0.78, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "store_opened_date"))
    .cube("corr_revenue_units_tier", "week_start")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.round(F.sum("corr_revenue_units_2") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("corr_revenue_units_2_per_chiang_mai"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
        F.grouping("corr_revenue_units_tier").alias("grouping_id"),
    )
)

result = (
    grouped
    .select("week_start", "revenue_p95", "east_rows", "grouping_id")
    .orderBy(F.col("east_rows").desc_nulls_last())
)
