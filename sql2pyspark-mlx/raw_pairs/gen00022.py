from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-07-09").cast("date"))
    .withColumn("brand_prefix", F.substring("brand", 1, 4))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price") > 300), "left_anti")
)

enriched = (
    not_flagged
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .filter(~F.col("d.country").isin(["JP", "SG", "TH"]))
    .select(
        "a.id",
        "a.category",
        "a.price",
        "a.cost",
        "a.launch_date",
        "a.supplier_id",
        "a.brand_prefix",
        F.col("d.supplier_name"),
        F.col("d.country"),
        F.col("d.rating"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.launch_date")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
        F.when(F.col("cost") >= 560, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.corr("price", "cost").alias("corr_price_cost"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.countDistinct("category").alias("distinct_categories"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .orderBy("num_categories")
)
