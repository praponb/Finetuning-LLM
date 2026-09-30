from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy(F.month("launch_date"), "supplier_id").orderBy(F.col("launch_date").desc(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("cost").alias("max_product_cost"))

filtered = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-04-15")))
    .select(
        "*",
        F.date_sub("launch_date", 44).alias("launch_date_minus_44d"),
        F.length("product_name").alias("product_name_length"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.brand",
        "a.price",
        "a.stock_qty",
        "a.launch_date",
        "a.supplier_id",
        F.col("p.city"),
        F.col("p.supplier_name"),
        F.col("p.rating"),
    )
)

scored = (
    enriched
    .withColumn("brand_avg_stock_qty", F.avg("stock_qty").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

with_windows = (
    scored
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "supplier_id",
        "rating",
        "brand_avg_stock_qty",
        F.max("brand_avg_stock_qty").over(w3).alias("last7_max_brand_avg_stock_qty"),
        F.min("brand_avg_stock_qty").over(w3).alias("last7_min_brand_avg_stock_qty"),
        F.avg("brand_avg_stock_qty").over(w3).alias("last7_avg_brand_avg_stock_qty"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue"))
)

stats = (
    latest_match
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 331, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["stock_qty_tier", "supplier_id"], ["stock_qty_tier"]],
        "stock_qty_tier",
        "supplier_id",
    )
    .agg(
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.countDistinct("category").alias("distinct_categories"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("stock_qty_per_home"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("rating")).otherwise(0)).alias("fashion_rating"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    stats
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "stock_qty_tier",
        "supplier_id",
        "corr_price_stock_qty",
        "distinct_categories",
        "stock_qty_per_home",
        "fashion_rating",
        "gid",
        F.col("max_product_cost"),
    )
    .orderBy(F.col("supplier_id").desc_nulls_first())
)
