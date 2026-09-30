from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.partitionBy(F.month("launch_date"), "supplier_id").orderBy("launch_date", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("category").orderBy("quantity_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id", "category")

with_dims = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.city"))
)

with_last = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

metrics = (
    with_last
    .select(
        "*",
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_2"),
        F.rank().over(w_running).alias("quantity_rank"),
        F.avg("stock_qty").over(w4).alias("stock_qty_avg_6_prior"),
    )
)

enriched = (
    metrics
    .filter(F.col("quantity") > 4)
    .select(
        "*",
        F.avg("quantity_rank").over(w5).alias("running_avg_quantity_rank"),
        F.max("quantity_rank").over(w5).alias("running_max_quantity_rank"),
        F.count("quantity_rank").over(w5).alias("running_count_quantity_rank"),
        F.sum("quantity_rank").over(w5).alias("running_sum_quantity_rank"),
        F.min("cost").over(w6).alias("supplier_id_min_cost"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("stock_qty_avg_6_prior")).otherwise(0)).alias("initech_stock_qty_avg_6_prior"),
        F.round(F.sum("running_avg_quantity_rank") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("running_avg_quantity_rank_per_globex"),
        F.count_if(F.col("city") == "London").alias("london_events"),
    )
    .orderBy(F.col("num_rows").desc())
)
