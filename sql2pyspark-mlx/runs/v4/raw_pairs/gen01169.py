from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("category")
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand")
w6 = Window.partitionBy("supplier_id").orderBy(F.col("brand_max_price").desc_nulls_last(), "id")

metrics = (
    df_products
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("cost").over(w2).alias("category_avg_cost"),
        F.sum("stock_qty").over(w3).alias("cumulative_stock_qty"),
        F.sum("cost").over(w4).alias("running_cost"),
        F.max("price").over(w5).alias("brand_max_price"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price") <= 230), "left_anti")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.country"), F.col("b.rating"), F.col("b.supplier_name"))
)

result = (
    enriched
    .withColumn("brand_max_price_tier", F.when(F.col("brand_max_price") >= 1069, "HIGH").otherwise("LOW"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 680, "PREMIUM").otherwise("STANDARD"))
    .cube("brand_max_price_tier", "cost_tier")
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.percentile_approx("cumulative_stock_qty", [0.25, 0.5, 0.75]).alias("cumulative_stock_qty_quartiles"),
        F.grouping("brand_max_price_tier").alias("gid"),
    )
)
