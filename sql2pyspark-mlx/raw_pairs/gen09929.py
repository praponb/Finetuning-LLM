from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.round("stock_qty", 0).alias("stock_qty_rounded"),
        F.concat("category", F.lit("/"), "product_name").alias("category_product_name"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

enriched = (
    filtered
    .filter(F.col("stock_qty_rounded") >= 72)
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "supplier_id",
        "stock_qty_rounded",
        F.max("stock_qty_rounded").over(w_ordered).alias("running_max_stock_qty_rounded"),
        F.count("stock_qty_rounded").over(w_ordered).alias("running_count_stock_qty_rounded"),
        F.min("stock_qty_rounded").over(w_ordered).alias("running_min_stock_qty_rounded"),
    )
)

kept_keys = (
    enriched
    .select("id", "supplier_id")
    .intersectAll(
        df_products
        .filter(F.col("cost").between(220, 510))
        .select("id", "supplier_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("id")
    .exceptAll(
        df_products
        .filter(F.col("cost") > 220)
        .select("id")
    )
)

kept_rows_products = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_products = (
    kept_rows_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .select(
        "e.cost",
        "e.supplier_id",
        "e.running_max_stock_qty_rounded",
        "e.running_min_stock_qty_rounded",
        F.col("s.city"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)

result = (
    enriched_products
    .groupBy(
        F.when(F.col("rating") >= 3.5, "HIGH").otherwise("LOW").alias("rating_tier"),
        F.when(F.col("running_min_stock_qty_rounded") >= 75, "HIGH").otherwise("LOW").alias("running_min_stock_qty_rounded_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.percentile_approx("running_min_stock_qty_rounded", [0.25, 0.5, 0.75]).alias("running_min_stock_qty_rounded_quartiles"),
        F.max(F.when(F.col("country") == "TH", F.col("running_min_stock_qty_rounded"))).alias("th_max_running_min_stock_qty_rounded"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
    .orderBy(F.col("us_events").desc())
)
