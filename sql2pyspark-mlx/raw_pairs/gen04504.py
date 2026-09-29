from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("city", "country").orderBy(F.col("cost").desc())
w_hist = Window.partitionBy("country")
w_recent = Window.partitionBy("city")
w5 = Window.orderBy(F.col("cost_cume_dist").desc_nulls_first())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_length", F.length("supplier_name"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"), F.col("c.cost"))
)

matched_keys = (
    with_latest
    .select("id", "city")
    .intersect(
        df_suppliers
        .filter(F.col("rating") >= 2.2)
        .select("id", "city")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("country").isin(["DE", "US"]))
    .select(
        "id",
        "city",
        "supplier_name_length",
        "product_stock_qty",
        F.cume_dist().over(w_part).alias("cost_cume_dist"),
        F.max("product_stock_qty").over(w_hist).alias("country_max_product_stock_qty"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("country_max_product_stock_qty").over(w_recent).alias("city_avg_country_max_product_stock_qty"),
        F.rank().over(w5).alias("cost_cume_dist_rank"),
        F.avg("product_stock_qty").over(w_recent).alias("city_avg_product_stock_qty"),
    )
)

kept_keys = (
    scored
    .select("city", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("supplier_name").like("Si%"))
        .select("city", "id")
    )
)

reconciled_suppliers = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled_suppliers
    .groupBy(
        F.when(F.col("city_avg_product_stock_qty") >= 312, "LARGE").otherwise("SMALL").alias("city_avg_product_stock_qty_tier"),
    )
    .agg(
        F.percentile_approx("supplier_name_length", 0.95).alias("supplier_name_length_p95"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
    )
    .orderBy(F.col("berlin_rows").desc_nulls_last())
)
