from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2024-02-24").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.greatest("stock_qty", "price").alias("greatest_stock_qty_price"),
        F.year("launch_date").alias("launch_date_year"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.count_if(F.col("category") == "Fashion").alias("fashion_events_2"),
        F.percentile_approx("launch_date_year", 0.5).alias("launch_date_year_p50"),
    )
)
