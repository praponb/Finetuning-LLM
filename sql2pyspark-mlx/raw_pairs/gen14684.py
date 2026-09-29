from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.orderBy("store_sale_units", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("stock_qty_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("store_sale_units")
w7 = Window.orderBy("store_sale_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("cumulative_stock_qty_plus_stock_qty_bucket_tier").orderBy(F.col("min_cumulative_stock_qty").desc())

valid = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Acme", "Stark"]))
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.length("category").alias("category_length"),
        F.trim("product_name").alias("clean_product_name"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.stock_qty",
        "s.tags",
        "s.brand_clean",
        F.col("c.units").alias("store_sale_units"),
    )
)

scored = (
    with_last
    .filter(~F.col("product_name").like("Co%"))
    .withColumn("stock_qty_bucket", F.ntile(4).over(w2))
    .withColumn("stock_qty_avg_5", F.avg("stock_qty").over(w3))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w4))
)

scored_products = (
    scored
    .select(
        "*",
        F.sum("cumulative_stock_qty").over(w4).alias("running_cumulative_stock_qty"),
        F.sum("store_sale_units").over(w5).alias("cumulative_store_sale_units"),
        F.rank().over(w6).alias("store_sale_units_rank"),
        F.sum("store_sale_units").over(w7).alias("running_store_sale_units"),
    )
)

filtered = (
    scored_products
    .filter(F.col("stock_qty").isNotNull())
    .withColumn("cumulative_stock_qty_plus_stock_qty_bucket", (F.col("cumulative_stock_qty") + F.col("stock_qty_bucket")))
    .withColumn("cumulative_stock_qty_filled", F.coalesce("cumulative_stock_qty", F.lit(0)))
)

matched_keys = (
    filtered
    .select("id")
    .subtract(
        df_products
        .filter(F.col("cost") < 600)
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("cumulative_stock_qty_plus_stock_qty_bucket") >= 76, "LARGE").otherwise("SMALL").alias("cumulative_stock_qty_plus_stock_qty_bucket_tier"),
        F.when(F.col("cumulative_stock_qty") >= 2083, "HIGH").otherwise("LOW").alias("cumulative_stock_qty_tier"),
    )
    .agg(
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.percentile_approx("stock_qty_avg_5", 0.95).alias("stock_qty_avg_5_p95"),
        F.min("cumulative_stock_qty").alias("min_cumulative_stock_qty"),
    )
    .withColumn("min_cumulative_stock_qty_rank", F.rank().over(w8))
    .orderBy(F.col("stock_qty_avg_5_p95").asc_nulls_first())
)
