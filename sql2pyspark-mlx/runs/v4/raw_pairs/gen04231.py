from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("revenue").desc_nulls_first())
w2 = Window.partitionBy("region")

remaining_keys = (
    df_store_sales
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([1, 3, 5, 6]))
        .select("store_id", "product_id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.brand"), F.col("p.price"))
)

with_dims_store_sales = (
    with_dims
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.price").alias("product_price"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
)

enriched = (
    with_dims_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.region"), F.col("s.city"), F.col("s.store_name"))
)

enriched_store_sales = (
    enriched
    .withColumn("revenue_percent_rank", F.percent_rank().over(w))
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w2))
)

with_dims_store_sales_2 = (
    enriched_store_sales
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.category"),
        F.col("p.cost"),
        F.col("p.launch_date").alias("product_launch_date"),
    )
)

enriched_store_sales_2 = (
    with_dims_store_sales_2
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.product_name"), F.col("b.brand").alias("product_brand"))
)

result = (
    enriched_store_sales_2
    .groupBy(
        F.when(F.col("product_price") >= 1070, "PREMIUM").otherwise("STANDARD").alias("product_price_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("product_price", 0.95).alias("product_price_p95"),
        F.round(F.sum("corr_revenue_units") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("corr_revenue_units_per_stark"),
    )
    .filter(F.col("corr_revenue_units_per_stark") > 0)
    .orderBy(F.col("distinct_store_ids").desc_nulls_first())
)
