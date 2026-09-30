from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id") == 5)
        .select("store_id", "product_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.year("sale_date") == 2025)
    .select(
        "id",
        "sale_date",
        "units",
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

valid = (
    filtered
    .filter((F.col("sale_date") > F.lit("2023-08-01").cast("date")) & (F.col("units") > 32))
    .select(
        "*",
        F.round((F.col("revenue_int") / F.col("units")), 2).alias("revenue_int_per_units"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

valid_store_sales = (
    valid
    .filter(F.col("sale_date") > F.lit("2023-06-25").cast("date"))
    .select(
        "*",
        F.add_months("sale_date_month_end", 1).alias("sale_date_month_end_plus_months"),
        F.round("units", 2).alias("units_rounded"),
    )
)

matched_keys = (
    valid_store_sales
    .select("id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([2, 5, 10]))
        .select("id")
    )
)

reconciled = (
    valid_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
        F.when(F.col("units_rounded") >= 77, "PREMIUM").otherwise("STANDARD").alias("units_rounded_tier"),
    )
    .agg(
        F.round(F.avg("revenue_int_per_units"), 2).alias("avg_revenue_int_per_units"),
        F.percentile_approx("revenue_int_per_units", [0.25, 0.5, 0.75]).alias("revenue_int_per_units_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("revenue_int", 0.5).alias("revenue_int_p50"),
    )
    .filter(F.col("revenue_int_p50") > 0)
)
