from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("category") == "Electronics")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.length("product_name").alias("product_name_length"),
        (F.col("price") - F.col("cost")).alias("margin"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    valid
    .withColumn("margin_tier", F.when(F.col("margin") >= 71, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["margin_tier", "category"], ["margin_tier"], []],
        "margin_tier",
        "category",
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .filter(F.col("num_brands") > 1)
)
