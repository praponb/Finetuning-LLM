from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.partitionBy("region")
w4 = Window.partitionBy("region").orderBy(F.col("next_store_sale_revenue").desc_nulls_first())
w5 = Window.orderBy(F.col("prev_store_sale_revenue").asc_nulls_last())
w6 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())
w7 = Window.partitionBy("region").orderBy(F.col("row_seq").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

metrics = (
    latest_match
    .filter(F.col("sqft") >= 14192)
    .withColumn("next_sqft", F.lead("sqft").over(w2))
    .withColumn("next_store_sale_revenue", F.lead("store_sale_revenue").over(w2))
    .withColumn("prev_store_sale_revenue", F.lag("store_sale_revenue").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
)

remaining_keys = (
    metrics
    .select("region")
    .intersect(
        df_stores
        .filter(F.col("region") == "Central")
        .select("region")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.customer_id").isin([2, 8, 13, 14])), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.cume_dist().over(w4).alias("next_store_sale_revenue_cume_dist"),
    )
)

scored = (
    with_windows
    .withColumn("region_avg_sqft", F.avg("sqft").over(w3))
    .withColumn("prev_store_sale_revenue_percent_rank", F.percent_rank().over(w5))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w6))
    .withColumn("row_seq_rank", F.rank().over(w7))
)

filtered = (
    scored
    .filter(F.col("opened_date") >= F.lit("2023-04-16").cast("date"))
    .select(
        "opened_date",
        "sqft",
        "store_sale_sale_date",
        "next_store_sale_revenue",
        "prev_store_sale_revenue",
        "row_seq",
        "next_store_sale_revenue_cume_dist",
        "prev_store_sale_revenue_percent_rank",
        "sqft_cume_dist",
        "row_seq_rank",
        F.length("region").alias("region_length"),
        F.coalesce("prev_store_sale_revenue_percent_rank", F.lit(0)).alias("prev_store_sale_revenue_percent_rank_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("next_store_sale_revenue") >= 3700, "PREMIUM").otherwise("STANDARD").alias("next_store_sale_revenue_tier"),
    )
    .agg(
        F.percentile_approx("next_store_sale_revenue", 0.9).alias("next_store_sale_revenue_p90"),
        F.round(F.avg("prev_store_sale_revenue_percent_rank_filled"), 2).alias("avg_prev_store_sale_revenue_percent_rank_filled"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
    .orderBy(F.col("next_store_sale_revenue_p90").desc_nulls_first())
)
