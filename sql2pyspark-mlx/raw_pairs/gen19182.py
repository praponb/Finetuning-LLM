from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy(F.month("launch_date"), "supplier_id")
w3 = Window.partitionBy("city").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("category", "city").orderBy("last7_avg_rating", "id")
w5 = Window.partitionBy("category").orderBy("last7_max_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.city"), F.col("p.rating"))
)

with_windows = (
    joined
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "city",
        "rating",
        F.max("rating").over(w_running).alias("last7_max_rating"),
        F.min("rating").over(w_running).alias("last7_min_rating"),
        F.avg("rating").over(w_running).alias("last7_avg_rating"),
        F.sum("rating").over(w_running).alias("last7_sum_rating"),
        F.corr("cost", "price").over(w_ordered).alias("corr_cost_price"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id").isin([20, 40])), "left_semi")
)

enriched = (
    kept
    .select(
        "*",
        F.avg("last7_max_rating").over(w3).alias("running_avg_last7_max_rating"),
        F.max("last7_max_rating").over(w3).alias("running_max_last7_max_rating"),
        F.count("last7_max_rating").over(w3).alias("running_count_last7_max_rating"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([2, 8, 9])), "left_anti")
)

enriched_products = (
    not_flagged
    .filter(F.col("city").isin(["Chiang Mai", "London"]))
    .select(
        "*",
        F.lead("stock_qty").over(w_hist).alias("next_stock_qty"),
        F.lead("last7_max_rating").over(w_hist).alias("next_last7_max_rating"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.sum("rating").over(w5).alias("cumulative_rating"),
    )
)

result = (
    enriched_products
    .select(
        "id",
        "product_name",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "city",
        "rating",
        "last7_max_rating",
        F.when(F.col("product_name").rlike("^(Office Chair|Desk Lamp)$"), "OTHER_ZONE").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "PRIORITY").otherwise("GROUP_A").alias("product_name_zone"),
    )
    .orderBy(F.col("cost").desc_nulls_last())
)
