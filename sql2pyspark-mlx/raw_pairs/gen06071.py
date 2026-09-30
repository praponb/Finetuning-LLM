from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("last4_min_sqft", "id")
w_part = Window.orderBy("last4_sum_sqft", "id")
w5 = Window.orderBy(F.col("city_count_last4_min_sqft").desc_nulls_last())

with_windows = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        F.count("sqft").over(w).alias("last4_count_sqft"),
        F.sum("sqft").over(w).alias("last4_sum_sqft"),
        F.min("sqft").over(w).alias("last4_min_sqft"),
    )
)

metrics = (
    with_windows
    .filter(F.col("last4_count_sqft") < 7)
    .select(
        "id",
        "store_name",
        "city",
        "last4_count_sqft",
        "last4_sum_sqft",
        "last4_min_sqft",
        F.round((F.col("last4_min_sqft") / F.sum("last4_min_sqft").over(w2)), 4).alias("last4_min_sqft_share"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date").between("2024-03-01", "2025-02-01")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") > "2024-08-05"), "left_semi")
)

enriched = (
    kept
    .select(
        "id",
        "last4_sum_sqft",
        "last4_min_sqft_share",
        "row_seq",
        F.round((F.col("last4_count_sqft") / F.sum("last4_count_sqft").over(w2)), 4).alias("last4_count_sqft_share"),
        F.count("last4_min_sqft").over(w2).alias("city_count_last4_min_sqft"),
    )
)

eligible_stores = (
    enriched
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id") == 1), "left_anti")
)

with_windows_stores = (
    eligible_stores
    .withColumn("next_last4_min_sqft_share", F.lead("last4_min_sqft_share").over(w_part))
    .withColumn("prev_last4_min_sqft_share", F.lag("last4_min_sqft_share").over(w_part))
    .withColumn("prev_row_seq", F.lag("row_seq").over(w_part))
    .withColumn("row_seq_2", F.row_number().over(w_part))
    .withColumn("city_count_last4_min_sqft_rank", F.rank().over(w5))
)

result = (
    with_windows_stores
    .groupBy(
        F.when(F.col("row_seq_2") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_2_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("last4_min_sqft_share"), 2).alias("avg_last4_min_sqft_share"),
        F.percentile_approx("prev_row_seq", [0.25, 0.5, 0.75]).alias("prev_row_seq_quartiles"),
    )
    .orderBy(F.col("row_seq_2_tier").asc_nulls_first())
)
