from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("brand", "category")
    .subtract(
        df_products
        .filter(F.col("brand").isin(["Stark", "Umbrella", "Initech"]))
        .select("brand", "category")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.max(F.when(F.col("category") == "Home", F.col("cost"))).alias("home_max_cost"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.percentile_approx("price", 0.9).alias("price_p90"),
    )
    .filter(F.col("home_max_cost") < 0)
)
