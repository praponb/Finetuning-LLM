from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy(F.month("sale_date"), "product_id").orderBy(F.col("revenue").desc_nulls_last())
w3 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w4 = Window.orderBy("units", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("revenue_bucket", "id")
w6 = Window.partitionBy("store_id")
w7 = Window.orderBy(F.col("revenue").asc_nulls_last())

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.ntile(5).over(w).alias("revenue_bucket"),
        F.cume_dist().over(w2).alias("revenue_cume_dist"),
        F.lag("revenue", 1).over(w3).alias("prev_revenue"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "*",
        F.avg("revenue_bucket").over(w4).alias("revenue_bucket_avg_2"),
        F.ntile(4).over(w5).alias("revenue_bucket_bucket"),
    )
)

matched_keys = (
    with_windows_store_sales
    .select("id", "store_id")
    .exceptAll(
        df_store_sales
        .filter(F.month("sale_date").isin([4, 7, 10]))
        .select("id", "store_id")
    )
)

reconciled = (
    with_windows_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.store_id",
        "a.revenue",
        "a.revenue_cume_dist",
        "a.prev_revenue",
        "a.revenue_bucket_bucket",
        F.col("p.opened_date"),
    )
)

joined_store_sales = (
    joined
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.opened_date").alias("store_opened_date"),
        F.col("d.store_name"),
    )
)

with_windows_store_sales_2 = (
    joined_store_sales
    .filter(F.col("store_name").like("%Rive%"))
    .withColumn("store_id_count_revenue_cume_dist", F.count("revenue_cume_dist").over(w6))
    .withColumn("revenue_cume_dist_2", F.cume_dist().over(w7))
)

joined_store_sales_2 = (
    with_windows_store_sales_2
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select(
        "e.store_id",
        "e.prev_revenue",
        F.col("b.opened_date").alias("store_opened_date_2"),
        F.col("b.city"),
        F.col("b.sqft"),
    )
)

result = (
    joined_store_sales_2
    .withColumn("activity_month", F.date_trunc("MONTH", "store_opened_date_2"))
    .cube("activity_month")
    .agg(
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.grouping_id().alias("gid"),
    )
)
