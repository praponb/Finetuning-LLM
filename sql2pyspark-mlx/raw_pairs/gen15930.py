from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w_hist = Window.partitionBy("order_id", "product_id").orderBy("unit_price_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("unit_price_adjusted").desc(), "id")
w4 = Window.partitionBy("product_id", "order_id").orderBy("line_total", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("product_id", "order_id").orderBy("unit_price_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id", "product_id").orderBy("cumulative_quantity", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("quantity") < 7)
    .select(
        "*",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        F.round((F.col("unit_price") * 100), 2).alias("unit_price_adjusted"),
    )
)

scored = (
    valid
    .filter(F.col("line_total").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "line_total",
        F.lag("unit_price", 1).over(w).alias("prev_unit_price"),
        F.sum("quantity").over(w_hist).alias("cumulative_quantity"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("unit_price_adjusted").over(w4).alias("unit_price_adjusted_avg_6_prior"),
        F.first("line_total", ignorenulls=True).over(w5).alias("first_known_line_total"),
    )
)

metrics = (
    scored
    .withColumn("last3_avg_unit_price_adjusted_avg_6_prior", F.avg("unit_price_adjusted_avg_6_prior").over(w6))
    .withColumn("last3_count_unit_price_adjusted_avg_6_prior", F.count("unit_price_adjusted_avg_6_prior").over(w6))
    .withColumn("last3_min_unit_price_adjusted_avg_6_prior", F.min("unit_price_adjusted_avg_6_prior").over(w6))
)

kept_keys = (
    metrics
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(160, 390))
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("last3_count_unit_price_adjusted_avg_6_prior") > 10)
    .withColumn("last3_min_unit_price_adjusted_avg_6_prior_rounded", F.round("last3_min_unit_price_adjusted_avg_6_prior", 0))
)

joined = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.order_id",
        "src.product_id",
        "src.line_total",
        "src.prev_unit_price",
        "src.event_seq",
        "src.unit_price_adjusted_avg_6_prior",
        "src.first_known_line_total",
        "src.last3_avg_unit_price_adjusted_avg_6_prior",
        "src.last3_count_unit_price_adjusted_avg_6_prior",
        "src.last3_min_unit_price_adjusted_avg_6_prior",
        "src.last3_min_unit_price_adjusted_avg_6_prior_rounded",
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("p.cost"),
    )
)

joined_order_items = (
    joined
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.order_id",
        "a.product_id",
        "a.line_total",
        "a.prev_unit_price",
        "a.unit_price_adjusted_avg_6_prior",
        "a.last3_avg_unit_price_adjusted_avg_6_prior",
        "a.last3_min_unit_price_adjusted_avg_6_prior",
        "a.last3_min_unit_price_adjusted_avg_6_prior_rounded",
        "a.category",
        "a.cost",
        F.col("d.price"),
    )
)

result = (
    joined_order_items
    .groupBy("product_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.percentile_approx("prev_unit_price", 0.9).alias("prev_unit_price_p90"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids_2"),
        F.sum(F.when(F.col("category") == "Sports", F.col("last3_min_unit_price_adjusted_avg_6_prior"))).alias("sports_total_last3_min_unit_price_adjusted_avg_6_prior"),
        F.sort_array(F.collect_set("category")).alias("category_list_2"),
    )
    .filter(F.col("prev_unit_price_p90") > 0)
    .orderBy(
        F.col("sports_total_last3_min_unit_price_adjusted_avg_6_prior").desc_nulls_last(),
    )
)
