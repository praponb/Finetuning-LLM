from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("brand")
w2 = Window.partitionBy("supplier_country")
w3 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_first())
w4 = Window.partitionBy("brand", "country")
w5 = Window.partitionBy("country").orderBy("rating_share")
w_running = Window.partitionBy("supplier_id")
w_recent = Window.partitionBy("brand").orderBy(F.col("stock_qty_dense_rank").desc_nulls_first())

kept_keys = (
    df_products
    .select("category")
    .exceptAll(
        df_products
        .filter(F.col("brand").isin(["Umbrella", "Acme"]))
        .select("category")
    )
)

verified = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.city"), F.col("p.rating"), F.col("p.country"))
)

remaining_keys = (
    joined
    .select("supplier_id", "brand")
    .exceptAll(
        df_products
        .filter(F.col("brand") == "Initech")
        .select("supplier_id", "brand")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.city") == "Berlin")
    .select(
        "e.*",
        F.col("p.rating").alias("supplier_rating"),
        F.col("p.country").alias("supplier_country"),
    )
)

scored = (
    enriched
    .select(
        "brand",
        "cost",
        "stock_qty",
        "supplier_id",
        "rating",
        "country",
        F.avg("price").over(w_hist).alias("brand_avg_price"),
        F.corr("price", "cost").over(w2).alias("corr_price_cost"),
        F.dense_rank().over(w3).alias("stock_qty_dense_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w4)), 4).alias("rating_share"),
    )
)

remaining_keys_2 = (
    scored
    .select("brand")
    .exceptAll(
        df_products
        .filter(F.col("category") != "Books")
        .select("brand")
    )
)

verified_products = (
    scored
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

enriched_products = (
    verified_products
    .select(
        "*",
        F.rank().over(w5).alias("rating_share_rank"),
        F.min("corr_price_cost").over(w_running).alias("supplier_id_min_corr_price_cost"),
        F.sum("corr_price_cost").over(w_hist).alias("brand_sum_corr_price_cost"),
        F.sum("stock_qty").over(w_hist).alias("brand_sum_stock_qty"),
        F.dense_rank().over(w_recent).alias("stock_qty_dense_rank_dense_rank"),
    )
)

result = (
    enriched_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.country").alias("supplier_country"),
        F.col("e.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.rating")).alias("max_rating"),
        F.max(F.col("e.cost")).alias("max_cost"),
    )
    .orderBy(F.col("supplier_country").desc())
)
