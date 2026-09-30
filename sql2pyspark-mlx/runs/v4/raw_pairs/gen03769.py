from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand")
w_ordered = Window.partitionBy("supplier_id", "category").orderBy("price_share", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("category").orderBy(F.col("price_share").desc(), "id")

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-11-09"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.when(F.col("brand").isin(["Umbrella", "Initech"]), "Priority").otherwise("Other").alias("brand_group"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.lag("stock_qty", 1).over(w).alias("prev_stock_qty"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.units").between(15, 16)), "left_anti")
)

scored_products = (
    eligible
    .select(
        "*",
        F.avg("stock_qty").over(w_ordered).alias("stock_qty_avg_3_prior"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

enriched = (
    scored_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.city"), F.col("d.supplier_name"))
)

joined = (
    enriched
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.supplier_name").alias("supplier_supplier_name"))
)

totals = (
    joined
    .groupBy(
        F.when(F.col("prev_stock_qty") >= 306, "PREMIUM").otherwise("STANDARD").alias("prev_stock_qty_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.percentile_approx("prev_stock_qty", 0.9).alias("prev_stock_qty_p90"),
    )
)

result = (
    totals
    .select("prev_stock_qty_tier", "new_york_count")
    .orderBy(F.col("new_york_count").desc_nulls_last())
)
