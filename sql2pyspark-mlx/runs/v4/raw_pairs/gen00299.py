from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_whole")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("days_since_sale_date").asc_nulls_first())
w3 = Window.partitionBy("store_id").orderBy("revenue_whole", "id")
w4 = Window.orderBy("revenue_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w6 = Window.orderBy("days_since_sale_date_rank_per_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("days_since_sale_date").desc(), "id")
w_recent = Window.partitionBy("city").orderBy("first_known_days_since_sale_date", "id")
w9 = Window.partitionBy("product_id", "store_id").orderBy("days_since_sale_date_rank", "id").rowsBetween(-6, -1)
w10 = Window.orderBy("least_first_known_days_since_sale_date_revenue_whole_cume_dist", "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-06-21"))) & F.col("sale_date").between("2024-01-10", "2024-04-19"))
    .select(
        "*",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.ceil("revenue").alias("revenue_whole"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("revenue_whole_cume_dist"),
        F.rank().over(w2).alias("days_since_sale_date_rank"),
        F.lag("revenue_whole", 1).over(w3).alias("prev_revenue_whole"),
        F.first("days_since_sale_date", ignorenulls=True).over(w4).alias("first_known_days_since_sale_date"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.city"))
)

cleaned = (
    joined
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-06-12"))) & F.col("city").isNotNull())
    .withColumn("days_since_sale_date_rank_per_units", F.round((F.col("days_since_sale_date_rank") / F.col("units")), 2))
    .withColumn("least_first_known_days_since_sale_date_revenue_whole_cume_dist", F.least("first_known_days_since_sale_date", "revenue_whole_cume_dist"))
)

metrics_store_sales = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
)

scored = (
    metrics_store_sales
    .select(
        "*",
        F.lag("days_since_sale_date", 2).over(w_recent).alias("prev_days_since_sale_date"),
        F.avg("first_known_days_since_sale_date").over(w9).alias("first_known_days_since_sale_date_avg_6_prior"),
        F.lag("units", 2).over(w10).alias("prev_units"),
    )
)

stats = (
    scored
    .withColumn("week_start", F.date_trunc("WEEK", "sale_date"))
    .groupingSets(
        [["week_start", "store_id", "product_id"], ["week_start", "store_id"], ["week_start"], []],
        "week_start",
        "store_id",
        "product_id",
    )
    .agg(
        F.percentile_approx("least_first_known_days_since_sale_date_revenue_whole_cume_dist", 0.9).alias("least_first_known_days_since_sale_date_revenue_whole_cume_dist_p90"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    stats
    .select(
        "store_id",
        "least_first_known_days_since_sale_date_revenue_whole_cume_dist_p90",
        "revenue_quartiles",
    )
    .orderBy(
        F.col("least_first_known_days_since_sale_date_revenue_whole_cume_dist_p90").asc_nulls_last(),
        F.col("store_id").desc(),
    )
)
