from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5, 6]))
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
)

joined = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.month(F.col("d.launch_date")).isin([2, 5, 8]))
    .select("a.*", F.col("d.brand"), F.col("d.product_name"))
)

enriched = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.sale_date",
        "s.units",
        "s.revenue",
        "s.revenue_per_unit",
        "s.brand",
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("p.cost"),
    )
)

base = (
    enriched
    .filter((F.col("sale_date") >= F.lit("2023-04-17").cast("date")) & (F.col("revenue_per_unit") >= 55))
    .withColumn("adjusted_price", F.round((F.col("price") * 1.15), 2))
)

valid = (
    base
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-04-21"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_per_unit",
        "brand",
        "stock_qty",
        "price",
        "cost",
        (F.col("cost") * F.col("price")).alias("margin"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

result = (
    valid
    .groupBy("store_id")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("price").alias("avg_price"),
        F.bit_or("units").alias("units_bit_or"),
        F.bit_and("units").alias("units_bit_and"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
    )
    .orderBy(F.col("store_id").asc_nulls_last())
)
