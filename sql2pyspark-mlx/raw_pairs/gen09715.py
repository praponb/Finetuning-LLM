from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("product_price", "id")
w_recent = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("rating", "id")
w_ordered = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("city").orderBy("product_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city").orderBy("cumulative_product_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("product_stock_qty_avg_2", "id")
w10 = Window.partitionBy("city").orderBy("product_stock_qty_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("city").orderBy("rating_avg_6_prior", "id").rowsBetween(-5, -1)

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating",
        F.col("c.stock_qty").alias("product_stock_qty"),
        F.col("c.price").alias("product_price"),
    )
)

valid = (
    with_last
    .filter(F.col("city") != "Singapore")
    .select(
        "id",
        "city",
        "rating",
        "product_stock_qty",
        "product_price",
        F.when(F.col("product_stock_qty") >= 326, "High").when(F.col("product_stock_qty") >= 146, "Medium").otherwise("Low").alias("product_stock_qty_band"),
        (F.col("product_price") / F.nullif("product_stock_qty", F.lit(0))).alias("product_price_per_product_stock_qty"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_windows = (
    valid
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("product_stock_qty_avg_2", F.avg("product_stock_qty").over(w_recent))
    .withColumn("seq_no_2", F.row_number().over(w4))
    .withColumn("rating_avg_6_prior", F.avg("rating").over(w_ordered))
    .withColumn("cumulative_product_stock_qty", F.sum("product_stock_qty").over(w6))
)

filtered = (
    with_windows
    .filter(F.col("rating_avg_6_prior") != 2)
    .select(
        "*",
        F.col("cumulative_product_stock_qty").cast("string").alias("cumulative_product_stock_qty_str"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "rating",
        "product_stock_qty",
        "product_price",
        "seq_no",
        "seq_no_2",
        "rating_avg_6_prior",
        "cumulative_product_stock_qty_str",
        F.sum("product_price_per_product_stock_qty").over(w7).alias("running_product_price_per_product_stock_qty"),
        F.sum("seq_no").over(w8).alias("cumulative_seq_no"),
        F.row_number().over(w9).alias("seq_no_3"),
        F.last("product_price", ignorenulls=True).over(w10).alias("last_known_product_price"),
        F.avg("product_price").over(w11).alias("product_price_avg_5_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "rating",
        "product_stock_qty",
        "product_price",
        "seq_no",
        "seq_no_2",
        "rating_avg_6_prior",
        "cumulative_seq_no",
        "seq_no_3",
    )
    .orderBy(F.col("rating_avg_6_prior").desc_nulls_first())
)
