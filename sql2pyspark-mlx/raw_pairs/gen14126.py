from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("stock_qty", "id")
w3 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "category").orderBy(F.col("cost").asc_nulls_first())
w5 = Window.partitionBy(F.year("launch_date"), "category").orderBy("price", "id")
w6 = Window.partitionBy("supplier_id").orderBy(F.col("cost_cume_dist").asc_nulls_first(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("category").orderBy("stock_qty_bucket", "id")
w9 = Window.partitionBy("supplier_country").orderBy("stock_qty_avg_5_prior", "id")
w10 = Window.orderBy(F.col("stock_qty_avg_5_prior").desc_nulls_first())
w11 = Window.partitionBy("category").orderBy(F.col("next_stock_qty").desc_nulls_last())
w12 = Window.partitionBy("category", "supplier_country").orderBy("cost_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("category").orderBy(F.col("stock_qty_per_th").desc())

with_windows = (
    df_products
    .filter(F.col("price") >= 320)
    .withColumn("running_price", F.sum("price").over(w))
    .withColumn("stock_qty_bucket", F.ntile(5).over(w2))
    .withColumn("stock_qty_avg_5_prior", F.avg("stock_qty").over(w3))
    .withColumn("cost_cume_dist", F.cume_dist().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.supplier_name"), F.col("d.rating"), F.col("d.country"))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched_products = (
    latest
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.supplier_name").alias("supplier_supplier_name"),
        F.col("b.country").alias("supplier_country"),
        F.col("b.city"),
    )
)

with_latest = (
    enriched_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.stock_qty_bucket",
        "s.stock_qty_avg_5_prior",
        "s.cost_cume_dist",
        "s.rating",
        "s.supplier_supplier_name",
        "s.supplier_country",
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

scored = (
    with_latest
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w_ordered))
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w_ordered))
    .withColumn("prev_stock_qty_avg_5_prior", F.lag("stock_qty_avg_5_prior").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("next_stock_qty_avg_5_prior", F.lead("stock_qty_avg_5_prior", 1, 0).over(w9))
)

metrics = (
    scored
    .withColumn("stock_qty_avg_5_prior_cume_dist", F.cume_dist().over(w10))
    .withColumn("next_stock_qty_dense_rank", F.dense_rank().over(w11))
    .withColumn("last_known_stock_qty_bucket", F.last("stock_qty_bucket", ignorenulls=True).over(w12))
)

summary = (
    metrics
    .groupBy(
        "category",
        F.when(F.col("price") >= 1140, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Books", 1)), F.lit(0)), 2).alias("stock_qty_per_books"),
        F.avg(F.when(F.col("category") == "Electronics", F.col("stock_qty_avg_5_prior"))).alias("electronics_avg_stock_qty_avg_5_prior"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("supplier_country") == "TH", 1)), F.lit(0)), 2).alias("stock_qty_per_th"),
    )
    .withColumn("stock_qty_per_th_percent_rank", F.percent_rank().over(w13))
)

result = (
    summary
    .select(
        "category",
        "price_tier",
        "stock_qty_per_books",
        "electronics_avg_stock_qty_avg_5_prior",
        "stock_qty_per_th",
    )
)
