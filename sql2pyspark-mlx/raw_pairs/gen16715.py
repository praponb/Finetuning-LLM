from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("rating_filled", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_ordered = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Singapore"))) & F.col("country").isin(["SG", "DE"]))
    .select("*", F.coalesce("rating", F.lit(0)).alias("rating_filled"))
)

with_windows = (
    base
    .select(
        "id",
        "city",
        "rating_filled",
        F.sum("rating_filled").over(w).alias("last7_sum_rating_filled"),
        F.min("rating_filled").over(w).alias("last7_min_rating_filled"),
        F.count("rating_filled").over(w).alias("last7_count_rating_filled"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.last7_sum_rating_filled",
        F.col("c.price"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.supplier_id").isNotNull()), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price").alias("product_price"), F.col("c.stock_qty"))
)

latest_match_suppliers = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.product_stock_qty", "s.stock_qty", F.col("c.cost"))
)

scored = (
    latest_match_suppliers
    .withColumn("running_avg_cost", F.avg("cost").over(w_ordered))
    .withColumn("running_sum_cost", F.sum("cost").over(w_ordered))
    .withColumn("running_count_cost", F.count("cost").over(w_ordered))
    .withColumn("last_known_product_stock_qty", F.last("product_stock_qty", ignorenulls=True).over(w_hist))
)

result = (
    scored
    .groupBy(
        F.when(F.col("product_stock_qty") >= 370, "HIGH").otherwise("LOW").alias("product_stock_qty_tier"),
        F.when(F.col("stock_qty") >= 438, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_sum_cost", [0.25, 0.5, 0.75]).alias("running_sum_cost_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.min("running_sum_cost").alias("min_running_sum_cost"),
        F.percentile_approx("running_count_cost", 0.5).alias("running_count_cost_p50"),
    )
)
