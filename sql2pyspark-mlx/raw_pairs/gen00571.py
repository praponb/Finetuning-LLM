from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").desc_nulls_first())
w3 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("revenue_avg_5_prior").desc_nulls_last())
w7 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w8 = Window.partitionBy("store_id").orderBy(F.col("revenue_avg_5_prior").desc(), "id")
w9 = Window.partitionBy("product_id").orderBy("revenue_dense_rank_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("store_id")
w11 = Window.partitionBy("store_id").orderBy("revenue_avg_5_prior_percent_rank", "id")
w12 = Window.orderBy("revenue_avg_5_prior_percent_rank", "id")

with_windows = (
    df_store_sales
    .withColumn("revenue_avg_5_prior", F.avg("revenue").over(w))
    .withColumn("revenue_dense_rank", F.dense_rank().over(w2))
)

filtered = (
    with_windows
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-06-30"))) & (F.col("store_id") == 5))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_avg_5_prior",
        "revenue_dense_rank",
        F.coalesce("revenue_avg_5_prior", F.lit(0)).alias("revenue_avg_5_prior_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("revenue").between(2200, 2300))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue_avg_5_prior",
        "revenue_dense_rank",
        "revenue_avg_5_prior_filled",
        F.avg("revenue_dense_rank").over(w3).alias("revenue_dense_rank_avg_5_prior"),
        F.sum("revenue").over(w4).alias("running_revenue"),
        F.avg("revenue_avg_5_prior").over(w5).alias("revenue_avg_5_prior_avg_6"),
        F.percent_rank().over(w6).alias("revenue_avg_5_prior_percent_rank"),
        F.lag("revenue_avg_5_prior_filled", 2).over(w7).alias("prev_revenue_avg_5_prior_filled"),
    )
)

with_windows_store_sales = (
    metrics
    .select(
        "id",
        "store_id",
        "revenue_avg_5_prior",
        "revenue_avg_5_prior_filled",
        "revenue_dense_rank_avg_5_prior",
        "running_revenue",
        "revenue_avg_5_prior_percent_rank",
        "prev_revenue_avg_5_prior_filled",
        F.row_number().over(w8).alias("event_seq"),
        F.last("revenue_avg_5_prior_filled", ignorenulls=True).over(w9).alias("last_known_revenue_avg_5_prior_filled"),
    )
)

cleaned = (
    with_windows_store_sales
    .filter(F.col("revenue_dense_rank_avg_5_prior") != 4)
    .select(
        "id",
        "store_id",
        "revenue_dense_rank_avg_5_prior",
        "running_revenue",
        "revenue_avg_5_prior_percent_rank",
        "event_seq",
        "last_known_revenue_avg_5_prior_filled",
        (F.col("running_revenue") + F.col("event_seq")).alias("running_revenue_plus_event_seq"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "store_id",
        "revenue_dense_rank_avg_5_prior",
        F.avg("running_revenue").over(w10).alias("store_id_avg_running_revenue"),
        F.ntile(3).over(w11).alias("revenue_avg_5_prior_percent_rank_bucket"),
        F.lead("revenue_avg_5_prior_percent_rank", 1).over(w12).alias("next_revenue_avg_5_prior_percent_rank"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.sqft"), F.col("p.region"), F.col("p.store_name"))
)

grouped = (
    joined
    .groupBy(
        F.when(F.col("revenue_dense_rank_avg_5_prior") >= 8, "PREMIUM").otherwise("STANDARD").alias("revenue_dense_rank_avg_5_prior_tier"),
    )
    .agg(
        F.percentile_approx("store_id_avg_running_revenue", [0.25, 0.5, 0.75]).alias("store_id_avg_running_revenue_quartiles"),
        F.sum(F.when(F.col("region") == "East", F.col("revenue_dense_rank_avg_5_prior")).otherwise(0)).alias("east_revenue_dense_rank_avg_5_prior"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") >= 0)
)

result = (
    grouped
    .select(
        "revenue_dense_rank_avg_5_prior_tier",
        "store_id_avg_running_revenue_quartiles",
    )
    .orderBy(F.col("revenue_dense_rank_avg_5_prior_tier").desc_nulls_first())
    .limit(20)
)
