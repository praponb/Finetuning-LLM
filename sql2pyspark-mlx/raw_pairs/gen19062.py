from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-5, -1)
w4 = Window.orderBy(F.col("stock_qty").desc_nulls_first())
w5 = Window.partitionBy("store_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id", "region").orderBy("last_restock", "id")
w7 = Window.orderBy("stock_qty", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("region")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-09-17"))) & F.col("quantity").between(171, 219))
    .withColumn("quantity_whole", F.ceil("quantity"))
)

joined = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.stock_qty"))
)

metrics = (
    joined
    .withColumn("product_id_sum_quantity_whole", F.sum("quantity_whole").over(w))
    .withColumn("store_id_max_stock_qty", F.max("stock_qty").over(w2))
    .withColumn("stock_qty_avg_5_prior", F.avg("stock_qty").over(w3))
    .withColumn("stock_qty_rank", F.rank().over(w4))
    .withColumn("first_known_stock_qty", F.first("stock_qty", ignorenulls=True).over(w5))
)

with_dims = (
    metrics
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isin(["Singapore", "Bangkok", "London"]))
    .select(
        "s.id",
        "s.store_id",
        "s.quantity",
        "s.last_restock",
        "s.stock_qty",
        "s.product_id_sum_quantity_whole",
        "s.stock_qty_avg_5_prior",
        "s.stock_qty_rank",
        F.col("p.opened_date"),
        F.col("p.region"),
        F.col("p.store_name"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "last_restock",
        "stock_qty",
        "product_id_sum_quantity_whole",
        "stock_qty_avg_5_prior",
        "stock_qty_rank",
        "opened_date",
        F.lead("stock_qty_avg_5_prior", 2).over(w6).alias("next_stock_qty_avg_5_prior"),
        F.avg("quantity").over(w7).alias("quantity_avg_3_prior"),
        F.round((F.col("stock_qty_rank") / F.sum("stock_qty_rank").over(w8)), 4).alias("stock_qty_rank_share"),
    )
)

result = (
    enriched
    .withColumn("week_start", F.date_trunc("WEEK", "last_restock"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .groupingSets(
        [["week_start", "quarter_start"], ["week_start"], []],
        "week_start",
        "quarter_start",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity_avg_3_prior", 0.9).alias("quantity_avg_3_prior_p90"),
        F.percentile_approx("next_stock_qty_avg_5_prior", [0.25, 0.5, 0.75]).alias("next_stock_qty_avg_5_prior_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
