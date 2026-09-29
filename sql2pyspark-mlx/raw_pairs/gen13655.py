from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("supplier_rating", "id")
w3 = Window.partitionBy("category")
w4 = Window.partitionBy("city", "category").orderBy("supplier_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country")
w_running = Window.orderBy("price", "id")
w7 = Window.partitionBy("supplier_country")
w8 = Window.partitionBy("category", "city").orderBy("price", "id")
w_hist = Window.partitionBy("supplier_id", "city").orderBy("store_sale_revenue", "id")
w_ordered = Window.partitionBy("country").orderBy("country_max_price", "id").rowsBetween(-5, -1)

with_dims = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.city") != "Singapore")
    .select("src.*", F.col("b.rating"), F.col("b.supplier_name"), F.col("b.country"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.price",
        "a.stock_qty",
        "a.supplier_id",
        "a.supplier_name",
        "a.country",
        F.col("d.city"),
        F.col("d.rating").alias("supplier_rating"),
    )
)

with_latest = (
    joined
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

enriched = (
    with_latest
    .withColumn("next_price", F.lead("price", 2).over(w2))
    .withColumn("category_avg_price", F.avg("price").over(w3))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w4))
    .withColumn("country_max_price", F.max("price").over(w5))
)

with_dims_products = (
    enriched
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id").eqNullSafe(F.col("s.id")), "inner")
    .select(
        "src.*",
        F.col("s.country").alias("supplier_country"),
        F.col("s.supplier_name").alias("supplier_supplier_name"),
    )
)

cleaned = (
    with_dims_products
    .filter(~F.col("supplier_country").eqNullSafe(F.lit("UK")))
    .select(
        "*",
        F.initcap(F.trim("category")).alias("category_title"),
        F.concat_ws("-", "category", "city").alias("category_city_key"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "category",
        "stock_qty",
        "supplier_name",
        "country",
        "next_price",
        "category_avg_price",
        "country_max_price",
        "supplier_country",
        "category_title",
        "category_city_key",
        F.lag("stock_qty", 2).over(w_running).alias("prev_stock_qty"),
        F.sum("next_price").over(w7).alias("supplier_country_sum_next_price"),
        F.lag("country_max_price", 1).over(w8).alias("prev_country_max_price"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.avg("stock_qty").over(w_ordered).alias("stock_qty_avg_5_prior"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("stock_qty") >= 376, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
        F.when(F.col("category_avg_price") >= 1060, "PREMIUM").otherwise("STANDARD").alias("category_avg_price_tier"),
        F.when(F.col("prev_country_max_price") >= 1274, "PREMIUM").otherwise("STANDARD").alias("prev_country_max_price_tier"),
    )
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count("*").alias("row_count"),
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("supplier_country")).alias("num_supplier_countries"),
    )
    .orderBy(F.col("stock_qty_tier").desc_nulls_first())
)
