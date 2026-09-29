from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("supplier_id", "category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("running_count_price", "id")
w3 = Window.orderBy(F.col("running_count_price").desc(), "id")
w4 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("cost").asc_nulls_last())
w6 = Window.partitionBy("country", "supplier_id")
w7 = Window.partitionBy("supplier_id")
w8 = Window.partitionBy(F.month("launch_date"))

metrics = (
    df_products
    .select(
        "*",
        F.sum("price").over(w_hist).alias("running_sum_price"),
        F.count("price").over(w_hist).alias("running_count_price"),
        F.min("price").over(w_hist).alias("running_min_price"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "running_count_price",
        "running_min_price",
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(5).over(w3).alias("running_count_price_bucket"),
        F.sum("running_sum_price").over(w4).alias("cumulative_running_sum_price"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id").eqNullSafe(F.col("p2.id")), "left")
    .select(
        "s.id",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.supplier_id",
        "s.running_count_price",
        "s.running_min_price",
        "s.event_seq",
        "s.running_count_price_bucket",
        F.col("p2.country"),
        F.col("p2.rating"),
    )
)

kept_keys = (
    with_dims
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.col("launch_date") > "2025-01-01")
        .select("supplier_id", "id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_products = (
    kept_rows
    .select(
        "price",
        "running_count_price",
        "running_min_price",
        "running_count_price_bucket",
        F.dense_rank().over(w_grp).alias("cost_dense_rank"),
        F.round((F.col("running_count_price") / F.sum("running_count_price").over(w6)), 4).alias("running_count_price_share"),
        F.round((F.col("running_count_price_bucket") / F.sum("running_count_price_bucket").over(w7)), 4).alias("running_count_price_bucket_share"),
        F.min("running_min_price").over(w8).alias("launch_date_month_min_running_min_price"),
        F.min("running_count_price").over(w6).alias("country_min_running_count_price"),
    )
)

cleaned = (
    enriched_products
    .filter(F.col("cost_dense_rank") == 7)
    .select(
        "price",
        "running_min_price",
        "running_count_price_share",
        "running_count_price_bucket_share",
        "launch_date_month_min_running_min_price",
        F.round("country_min_running_count_price", 0).alias("country_min_running_count_price_rounded"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("launch_date_month_min_running_min_price") >= 1313, "LARGE").otherwise("SMALL").alias("launch_date_month_min_running_min_price_tier"),
        F.when(F.col("running_min_price") >= 1000, "HIGH").otherwise("LOW").alias("running_min_price_tier"),
        F.when(F.col("running_count_price_bucket_share") >= 0.74, "PREMIUM").otherwise("STANDARD").alias("running_count_price_bucket_share_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("running_count_price_share"), 2).alias("avg_running_count_price_share"),
    )
    .orderBy(F.col("running_min_price_tier").desc_nulls_first())
)
