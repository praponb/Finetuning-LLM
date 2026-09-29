from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("supplier_id", "brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy(F.date_trunc("month", "launch_date"), "category")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("category").orderBy("unit_price", "id")
w_grp = Window.partitionBy(F.year("launch_date"), "supplier_id").orderBy(F.col("stock_qty").desc_nulls_first())
w6 = Window.partitionBy("category")

enriched = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("running_avg_cost", F.avg("cost").over(w_running))
    .withColumn("running_max_cost", F.max("cost").over(w_running))
    .withColumn("running_sum_cost", F.sum("cost").over(w_running))
    .withColumn("running_count_cost", F.count("cost").over(w_running))
    .withColumn("corr_stock_qty_cost", F.corr("stock_qty", "cost").over(w_running_2))
)

eligible = (
    enriched
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.launch_date")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

result = (
    latest_match
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "running_sum_cost",
        "running_count_cost",
        "quantity",
        "unit_price",
        F.lead("price", 2).over(w_hist).alias("next_price"),
        F.percent_rank().over(w_grp).alias("stock_qty_percent_rank"),
        F.count("running_avg_cost").over(w6).alias("category_count_running_avg_cost"),
    )
)
