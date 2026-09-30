from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "store_id")
w4 = Window.partitionBy(F.date_trunc("MONTH", "sale_date"), "product_id").orderBy(F.col("cost_share_2").desc())
w5 = Window.orderBy("cost_share_times_cost")

remaining_keys = (
    df_store_sales
    .select("product_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue").isNotNull())
        .select("product_id")
    )
)

kept_rows = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select(
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.revenue",
        F.col("s.region"),
        F.col("s.store_name"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.cost"))
)

scored = (
    joined
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w)), 4))
    .withColumn("cost_cume_dist", F.cume_dist().over(w2))
    .withColumn("cost_share_2", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
)

base = (
    scored
    .filter(F.col("sale_date") > F.lit("2023-03-28").cast("date"))
    .withColumn("greatest_cost_cume_dist_cost_share_2", F.greatest("cost_cume_dist", "cost_share_2"))
    .withColumn("cost_share_times_cost", (F.col("cost_share") * F.col("cost")))
)

with_windows = (
    base
    .withColumn("cost_share_2_percent_rank", F.percent_rank().over(w4))
    .withColumn("cost_share_times_cost_cume_dist", F.cume_dist().over(w5))
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        F.when(F.col("revenue") >= 3700, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("cost_cume_dist", 0.5).alias("cost_cume_dist_p50"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.max(F.when(F.col("region") == "South", F.col("cost_share_times_cost_cume_dist"))).alias("south_max_cost_share_times_cost_cume_dist"),
        F.sort_array(F.collect_set("region")).alias("region_list_2"),
        F.round(F.avg("cost_share_2"), 2).alias("avg_cost_share_2"),
        F.sort_array(F.collect_set("region")).alias("region_list_3"),
    )
    .filter(F.col("avg_cost_share_2") >= 0)
    .orderBy(F.col("revenue_tier").desc_nulls_last())
)
