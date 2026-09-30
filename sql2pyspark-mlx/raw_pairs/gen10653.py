from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand")
w3 = Window.partitionBy("brand").orderBy(F.col("category_sum_price").desc_nulls_first(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("supplier_id").orderBy("store_sale_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country", "supplier_id").orderBy("store_sale_revenue", "id")
w_running = Window.partitionBy("country").orderBy("running_min_running_max_price", "id")
w8 = Window.orderBy("seq_no", "id")
w9 = Window.orderBy("running_count_running_max_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("country")
w11 = Window.partitionBy("country").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_products
    .select(
        "*",
        F.min("price").over(w_grp).alias("running_min_price"),
        F.avg("price").over(w_grp).alias("running_avg_price"),
        F.max("price").over(w_grp).alias("running_max_price"),
        F.count("price").over(w_grp).alias("running_count_price"),
        F.sum("price").over(w2).alias("category_sum_price"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .filter(F.col("d.rating") >= 4.1)
    .select("s.*", F.col("d.supplier_name"), F.col("d.country"))
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "product_name",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "running_min_price",
        "running_max_price",
        "supplier_name",
        "country",
    )
)

with_last = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "product_name",
        "country",
        "store_sale_revenue",
        F.count("running_max_price").over(w5).alias("running_count_running_max_price"),
        F.sum("running_max_price").over(w5).alias("running_sum_running_max_price"),
        F.max("running_max_price").over(w5).alias("running_max_running_max_price"),
        F.min("running_max_price").over(w5).alias("running_min_running_max_price"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

with_windows = (
    scored
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("running_running_count_running_max_price", F.sum("running_count_running_max_price").over(w9))
    .withColumn("running_sum_running_max_price_share", F.round((F.col("running_sum_running_max_price") / F.sum("running_sum_running_max_price").over(w10)), 4))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w11))
)

stats = (
    with_windows
    .groupBy(
        "seq_no",
        F.when(F.col("running_sum_running_max_price_share") >= 0.77, "HIGH").otherwise("LOW").alias("running_sum_running_max_price_share_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.avg("running_count_running_max_price").alias("avg_running_count_running_max_price"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    stats
    .select(
        "seq_no",
        "running_sum_running_max_price_share_tier",
        "row_count",
        "seq_no_p95",
        "country_list",
    )
    .orderBy(F.col("seq_no").desc())
)
