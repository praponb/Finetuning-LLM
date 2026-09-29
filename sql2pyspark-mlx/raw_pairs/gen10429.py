from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("units", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w_grp = Window.partitionBy("product_id", "store_id").orderBy("event_seq", "id")
w_ordered = Window.partitionBy(F.month("sale_date"), "store_id").orderBy("sale_date", "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.stock_qty") == 290)
    .select("e.*", F.col("b.brand"), F.col("b.product_name"), F.col("b.stock_qty"))
)

scored = (
    with_dims
    .withColumn("prev_units", F.lag("units").over(w))
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

enriched = (
    scored
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.stock_qty").alias("product_stock_qty"),
        F.col("d.price"),
        F.col("d.brand").alias("product_brand"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("product_stock_qty").over(w_grp).alias("next_product_stock_qty"),
        F.lag("product_stock_qty").over(w_grp).alias("prev_product_stock_qty"),
        F.lag("stock_qty").over(w_grp).alias("prev_stock_qty"),
        F.row_number().over(w_grp).alias("seq_no_2"),
    )
)

scored_store_sales = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "product_name",
        "stock_qty",
        "prev_units",
        "next_stock_qty",
        "event_seq",
        "price",
        "product_brand",
        "next_product_stock_qty",
        "prev_stock_qty",
        "seq_no_2",
        F.row_number().over(w_ordered).alias("event_seq_2"),
        F.last("next_stock_qty", ignorenulls=True).over(w5).alias("last_known_next_stock_qty"),
        F.sum("event_seq").over(w6).alias("cumulative_event_seq"),
    )
)

matched_keys = (
    scored_store_sales
    .select("id", "product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units").between(32, 37))
        .select("id", "product_id")
    )
)

kept_rows = (
    scored_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .groupingSets(
        [["brand", "product_brand"], ["brand"], []],
        F.col("p.brand").alias("brand"),
        F.col("e.product_brand").alias("product_brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.prev_stock_qty")).alias("avg_prev_stock_qty"),
        F.min(F.col("e.seq_no_2")).alias("min_seq_no_2"),
    )
)
