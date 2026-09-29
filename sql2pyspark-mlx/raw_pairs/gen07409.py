from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("revenue_whole", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy(F.year("opened_date"), "store_id").orderBy(F.col("revenue_whole").desc(), "id")
w3 = Window.orderBy(F.col("last4_count_revenue_whole").desc())
w4 = Window.partitionBy(F.month("opened_date")).orderBy("revenue_whole", "id")

base = (
    df_store_sales
    .filter(F.col("units") > 18)
    .withColumn("months_since_sale_date", (F.datediff(F.current_date(), "sale_date") / 30))
    .withColumn("revenue_whole", F.floor("revenue"))
)

joined = (
    base
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.region") == "North")
    .select("e.*", F.col("d.city"), F.col("d.store_name"), F.col("d.opened_date"))
)

enriched = (
    joined
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.revenue_whole",
        "a.store_name",
        "a.opened_date",
        F.col("d.sqft"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "revenue_whole",
        "store_name",
        "opened_date",
        F.min("revenue_whole").over(w_ordered).alias("last4_min_revenue_whole"),
        F.count("revenue_whole").over(w_ordered).alias("last4_count_revenue_whole"),
        F.max("revenue_whole").over(w_ordered).alias("last4_max_revenue_whole"),
        F.avg("revenue_whole").over(w_ordered).alias("last4_avg_revenue_whole"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

enriched_store_sales = (
    scored
    .select(
        "id",
        "last4_min_revenue_whole",
        "last4_count_revenue_whole",
        "last4_avg_revenue_whole",
        F.dense_rank().over(w3).alias("last4_count_revenue_whole_dense_rank"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    enriched_store_sales
    .withColumn("last4_count_revenue_whole_tier", F.when(F.col("last4_count_revenue_whole") >= 14, "LARGE").otherwise("SMALL"))
    .rollup("last4_count_revenue_whole_tier")
    .agg(
        F.percentile_approx("last4_count_revenue_whole_dense_rank", [0.25, 0.5, 0.75]).alias("last4_count_revenue_whole_dense_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("distinct_ids").desc())
)
