from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("category", "supplier_id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("cost").asc_nulls_first())
w4 = Window.partitionBy("supplier_id").orderBy(F.col("rating").desc())
w_recent = Window.orderBy("cost")
w6 = Window.partitionBy("supplier_id")
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("stock_qty").alias("min_product_stock_qty"))

remaining_keys = (
    df_products
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("stock_qty") >= 193)
        .select("id", "supplier_id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.launch_date")), "left_anti")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.rating"), F.col("d.supplier_name"))
)

scored = (
    enriched
    .withColumn("rating_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("category_stddev_samp_rating", F.stddev_samp("rating").over(w2))
    .withColumn("cost_dense_rank", F.dense_rank().over(w3))
    .withColumn("rating_rank", F.rank().over(w4))
    .withColumn("cost_dense_rank_2", F.dense_rank().over(w_recent))
)

with_windows = (
    scored
    .select(
        "*",
        F.corr("price", "stock_qty").over(w2).alias("corr_price_stock_qty"),
        F.sum("rating_rank").over(w6).alias("supplier_id_sum_rating_rank"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left_anti")
)

pivoted = (
    eligible
    .groupBy("supplier_id")
    .pivot("brand", ["Globex", "Initech"])
    .agg(F.avg("cost"))
)

result = (
    pivoted
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select("supplier_id", "Globex", "Initech", F.col("min_product_stock_qty"))
)
