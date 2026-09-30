from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-04-02").cast("date"))
    .select(
        "*",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

kept_keys = (
    base
    .select("product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([1, 4, 9]))
        .select("product_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.sqft"), F.col("p2.opened_date"))
)

remaining_keys = (
    with_dims
    .select("id", "store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("sale_date").between("2023-08-01", "2024-11-01"))
        .select("id", "store_id")
    )
)

reconciled_store_sales = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled_store_sales
    .withColumn("year_start", F.date_trunc("YEAR", "sale_date"))
    .rollup("year_start")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.count("*").alias("row_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.max("units").alias("max_units"),
        F.count("*").alias("event_count_2"),
        F.grouping("year_start").alias("grouping_id"),
    )
    .orderBy(F.col("year_start").desc())
)
