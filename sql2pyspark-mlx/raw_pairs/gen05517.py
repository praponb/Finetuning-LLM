from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("cost").desc(), "id")
w_hist = Window.partitionBy("city").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("stock_qty").desc_nulls_last())
w4 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-3, -1)

joined = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.city"), F.col("s.supplier_name"), F.col("s.country"))
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "city",
    )
)

kept = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.revenue") >= 2300), "left_anti")
)

with_windows = (
    kept
    .filter(F.col("brand") != "Initech")
    .select(
        "*",
        F.sum("stock_qty").over(w_hist).alias("cumulative_stock_qty"),
        F.rank().over(w3).alias("stock_qty_rank"),
        F.avg("stock_qty").over(w4).alias("stock_qty_avg_3_prior"),
    )
)

result = (
    with_windows
    .groupBy("brand")
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("stock_qty_per_tokyo"),
        F.percentile_approx("cumulative_stock_qty", 0.5).alias("cumulative_stock_qty_p50"),
        F.percentile_approx("stock_qty", 0.95).alias("stock_qty_p95"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
    )
    .filter(F.col("stock_qty_per_tokyo") > 0)
    .orderBy(F.col("stock_qty_p95").desc())
)
