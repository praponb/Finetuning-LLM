from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("product_id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("units").between(14, 29))
        .select("product_id", "store_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupingSets(
        [["product_id", "store_id"], ["product_id"], []],
        "product_id",
        "store_id",
    )
    .agg(
        F.stddev_samp("units").alias("units_stddev"),
        F.percentile_approx("revenue", 0.9).alias("revenue_p90"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
    )
    .filter(F.col("units_stddev") > 0)
)
