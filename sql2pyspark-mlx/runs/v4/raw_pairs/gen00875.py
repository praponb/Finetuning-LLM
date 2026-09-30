from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("price").desc(), "id")
w2 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("cost", "id")
w4 = Window.partitionBy("category").orderBy(F.col("last_known_stock_qty").desc())
w5 = Window.partitionBy("brand")
w6 = Window.orderBy(F.col("price").desc_nulls_first())
w7 = Window.orderBy(F.col("stock_qty").desc_nulls_last())

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price").between(220, 310)), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.ntile(5).over(w).alias("price_bucket"),
        F.last("stock_qty", ignorenulls=True).over(w2).alias("last_known_stock_qty"),
        F.ntile(10).over(w_running).alias("cost_bucket"),
    )
)

matched_keys = (
    scored
    .select("id")
    .intersectAll(
        df_products
        .filter(F.col("cost").between(310, 430))
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored_products = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w4).alias("last_known_stock_qty_dense_rank"),
        F.round((F.col("price_bucket") / F.sum("price_bucket").over(w5)), 4).alias("price_bucket_share"),
        F.percent_rank().over(w6).alias("price_percent_rank"),
        F.percent_rank().over(w7).alias("stock_qty_percent_rank"),
    )
)

matched_keys_2 = (
    scored_products
    .select("category", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select("category", "supplier_id")
    )
)

verified = (
    scored_products
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup(
        "category",
        F.when(F.col("price") >= 910, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.round(F.sum("price_percent_rank") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("price_percent_rank_per_globex"),
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.max(F.when(F.col("category") == "Home", F.col("last_known_stock_qty_dense_rank"))).alias("home_max_last_known_stock_qty_dense_rank"),
        F.max(F.when(F.col("category") == "Sports", F.col("price_bucket_share"))).alias("sports_max_price_bucket_share"),
    )
    .filter(F.col("home_max_last_known_stock_qty_dense_rank") < 0)
)
