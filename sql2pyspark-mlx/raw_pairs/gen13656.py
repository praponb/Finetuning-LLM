from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("stock_qty").asc_nulls_first())
w2 = Window.partitionBy(F.year("launch_date"), "category")
w3 = Window.orderBy("corr_cost_stock_qty")
w4 = Window.orderBy(F.col("price").desc_nulls_first())
w5 = Window.orderBy(F.col("stock_qty").asc_nulls_last())
w6 = Window.partitionBy(F.year("launch_date"), "category").orderBy(F.col("corr_cost_stock_qty_dense_rank").desc_nulls_last())
w7 = Window.partitionBy("category", "city")
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("supplier_id")
w_recent = Window.partitionBy("category")

kept_keys = (
    df_products
    .select("id", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isin([2, 15, 31, 33]))
        .select("id", "supplier_id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w2))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.launch_date")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.corr_cost_stock_qty",
        F.col("d.city"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "corr_cost_stock_qty",
        F.dense_rank().over(w3).alias("corr_cost_stock_qty_dense_rank"),
        F.cume_dist().over(w4).alias("price_cume_dist"),
        F.dense_rank().over(w5).alias("stock_qty_dense_rank"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .filter(F.col("p.rating").isNotNull())
    .select("s.*", F.col("p.country"), F.col("p.supplier_name"), F.col("p.city"))
)

metrics_products = (
    joined
    .select(
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "corr_cost_stock_qty",
        "price_cume_dist",
        "stock_qty_dense_rank",
        "country",
        F.rank().over(w6).alias("corr_cost_stock_qty_dense_rank_rank"),
        F.stddev_samp("corr_cost_stock_qty").over(w7).alias("category_stddev_samp_corr_cost_stock_qty"),
        F.count("price_cume_dist").over(w8).alias("city_count_price_cume_dist"),
        F.round((F.col("corr_cost_stock_qty_dense_rank") / F.sum("corr_cost_stock_qty_dense_rank").over(w9)), 4).alias("corr_cost_stock_qty_dense_rank_share"),
        F.corr("stock_qty", "cost").over(w_recent).alias("corr_stock_qty_cost"),
    )
)

result = (
    metrics_products
    .withColumn("corr_cost_stock_qty_tier", F.when(F.col("corr_cost_stock_qty") >= 0.72, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["country", "corr_cost_stock_qty_tier"], ["country"]],
        "country",
        "corr_cost_stock_qty_tier",
    )
    .agg(
        F.round(F.sum("stock_qty_dense_rank") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("stock_qty_dense_rank_per_electronics"),
        F.round(F.sum("corr_stock_qty_cost") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("corr_stock_qty_cost_per_uk"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 1000)
)
