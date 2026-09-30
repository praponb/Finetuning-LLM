from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")
w_running = Window.partitionBy("region")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.avg("revenue").over(w).alias("revenue_avg_2_prior"),
        F.ntile(4).over(w2).alias("revenue_bucket"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.sale_date",
        "s.units",
        "s.revenue",
        "s.revenue_avg_2_prior",
        "s.dedup_rank",
        F.col("p2.store_name"),
        F.col("p2.region"),
    )
)

remaining_keys = (
    joined
    .select("store_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") != 20)
        .select("store_id", "id")
    )
)

verified = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("revenue_avg_2_prior") <= 1855)
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "revenue_avg_2_prior",
        "dedup_rank",
        F.avg("units").over(w_running).alias("region_avg_units"),
        F.sum("revenue").over(w_running).alias("region_sum_revenue"),
    )
)

result = scored.select("id", "sale_date", "revenue", "dedup_rank", "region_sum_revenue")
