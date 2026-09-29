from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-21").cast("date"))
    .select("*", (F.col("units") + F.col("revenue")).alias("revenue_per_unit"))
)

remaining_keys = (
    cleaned
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id") == 5)
        .select("product_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.units",
        F.col("d.cost"),
        F.col("d.product_name"),
        F.col("d.brand"),
    )
)

cleaned_store_sales = (
    joined
    .filter(F.col("sale_date") > F.lit("2023-09-24").cast("date"))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

result = (
    cleaned_store_sales
    .groupBy("product_id", F.date_trunc("WEEK", "sale_date").alias("week_start"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .orderBy(F.col("umbrella_count").desc_nulls_last())
)
