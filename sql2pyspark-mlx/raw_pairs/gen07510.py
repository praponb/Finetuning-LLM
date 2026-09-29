from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("supplier_id")
w_running = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-05").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("launch_date_quarter", F.quarter("launch_date"))
    .withColumn("is_acme", F.when(F.col("brand") == "Acme", 1).otherwise(0))
)

enriched = (
    cleaned
    .filter(F.col("is_acme") < 62)
    .select(
        "id",
        "product_name",
        "stock_qty",
        "launch_date",
        "launch_date_quarter",
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("cost_share").over(w_running).alias("running_avg_cost_share"),
        F.sum("cost_share").over(w_running).alias("running_sum_cost_share"),
        F.min("cost_share").over(w_running).alias("running_min_cost_share"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.event_seq",
        "s.running_avg_cost_share",
        "s.running_sum_cost_share",
        F.col("c.sale_date"),
    )
)

matched_keys = (
    with_last
    .select("id")
    .intersectAll(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_sum_cost_share", 0.9).alias("running_sum_cost_share_p90"),
        F.min("event_seq").alias("min_event_seq"),
        F.percentile_approx("running_avg_cost_share", [0.25, 0.5, 0.75]).alias("running_avg_cost_share_quartiles"),
        F.min("event_seq").alias("min_event_seq_2"),
        F.round(F.avg("running_avg_cost_share"), 2).alias("avg_running_avg_cost_share"),
    )
    .orderBy(F.col("running_sum_cost_share_p90").desc())
)
