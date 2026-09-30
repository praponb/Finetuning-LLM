from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-07-19").cast("date")) & F.col("supplier_id").isNotNull() & F.col("category").isin(["Books", "Home", "Fashion"]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("price") * 100), 2).alias("price_adjusted"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("cost") >= 700, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("cost_per_electronics"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .filter(F.col("cost_per_electronics") >= 0)
)
