from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.orderBy("product_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        F.col("c.launch_date"),
        F.col("c.price").alias("product_price"),
    )
)

enriched = (
    latest_match
    .filter(F.year("launch_date") == 2024)
    .select(
        "id",
        "launch_date",
        "product_price",
        F.min("product_price").over(w2).alias("running_min_product_price"),
        F.sum("product_price").over(w2).alias("running_sum_product_price"),
        F.max("product_price").over(w2).alias("running_max_product_price"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "launch_date",
        "product_price",
        "running_sum_product_price",
        "running_max_product_price",
        F.min("product_price").over(w_recent).alias("running_min_product_price_2"),
        F.sum("product_price").over(w_recent).alias("running_sum_product_price_2"),
        F.max("product_price").over(w_recent).alias("running_max_product_price_2"),
        F.avg("product_price").over(w_recent).alias("running_avg_product_price"),
    )
)

remaining_keys = (
    with_windows
    .select("id")
    .intersect(
        df_suppliers
        .filter(F.col("rating") <= 3.2)
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("running_min_product_price_2") <= 780)
    .select(
        "id",
        "launch_date",
        "product_price",
        "running_sum_product_price",
        "running_max_product_price",
        "running_min_product_price_2",
        "running_sum_product_price_2",
        "running_max_product_price_2",
        "running_avg_product_price",
        F.greatest("running_sum_product_price_2", "running_max_product_price").alias("greatest_running_sum_product_price_2_running_max_product_price"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") >= F.col("s.launch_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= F.col("s.launch_date")), "left_semi")
)

summary = (
    eligible
    .groupBy(F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.percentile_approx("running_sum_product_price", 0.95).alias("running_sum_product_price_p95"),
        F.percentile_approx("running_max_product_price", 0.95).alias("running_max_product_price_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_min_product_price_2", 0.5).alias("running_min_product_price_2_p50"),
        F.percentile_approx("running_max_product_price_2", 0.5).alias("running_max_product_price_2_p50"),
        F.percentile_approx("running_sum_product_price_2", 0.5).alias("running_sum_product_price_2_p50"),
    )
    .filter(F.col("running_sum_product_price_p95") > 0)
)

result = (
    summary
    .select(
        "week_start",
        "running_sum_product_price_p95",
        "running_max_product_price_p95",
        "running_min_product_price_2_p50",
        "running_max_product_price_2_p50",
        "running_sum_product_price_2_p50",
    )
)
