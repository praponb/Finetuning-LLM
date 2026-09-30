from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.orderBy("stock_qty", "id")
w3 = Window.partitionBy("supplier_id").orderBy("running_count_rating", "id")
w4 = Window.orderBy("running_count_rating", "id")
w5 = Window.orderBy("rating", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-04-19"))) & F.col("brand").isNotNull())
    .withColumn("brand_prefix", F.substring("brand", 1, 3))
    .withColumn("margin", (F.col("price") * F.col("cost")))
)

valid = (
    cleaned
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-30"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "brand_prefix",
        "margin",
        F.trim("product_name").alias("clean_product_name"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "a.id",
        "a.product_name",
        "a.cost",
        "a.stock_qty",
        "a.tags",
        "a.supplier_id",
        "a.brand_prefix",
        "a.resolved_brand",
        F.col("p.supplier_name"),
        F.col("p.rating"),
    )
)

metrics = (
    with_dims
    .filter(F.col("supplier_name").like("Pa%"))
    .withColumn("running_avg_rating", F.avg("rating").over(w_grp))
    .withColumn("running_sum_rating", F.sum("rating").over(w_grp))
    .withColumn("running_count_rating", F.count("rating").over(w_grp))
    .withColumn("running_max_rating", F.max("rating").over(w_grp))
    .withColumn("next_stock_qty", F.lead("stock_qty", 2).over(w_grp_2))
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "product_name",
        "resolved_brand",
        "supplier_name",
        "rating",
        "running_count_rating",
        "running_max_rating",
    )
)

metrics_products = (
    deduped
    .select(
        "id",
        "supplier_name",
        "running_count_rating",
        F.row_number().over(w4).alias("event_seq"),
        F.lead("running_max_rating", 1).over(w5).alias("next_running_max_rating"),
    )
)

with_latest = (
    metrics_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.running_count_rating",
        "s.event_seq",
        "s.next_running_max_rating",
        F.col("c.sale_date"),
    )
)

result = (
    with_latest
    .groupBy(
        F.date_trunc("YEAR", "sale_date").alias("year_start"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("next_running_max_rating", [0.25, 0.5, 0.75]).alias("next_running_max_rating_quartiles"),
        F.percentile_approx("next_running_max_rating", 0.9).alias("next_running_max_rating_p90"),
    )
)
