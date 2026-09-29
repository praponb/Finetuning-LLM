from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("city", "brand").orderBy("city_sum_rating", "id")

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

scored = (
    with_last
    .withColumn("city_sum_rating", F.sum("rating").over(w2))
    .withColumn("rating_bucket", F.ntile(4).over(w3))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w4)), 4))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") <= 610), "left_anti")
)

with_last_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand"))
)

with_windows = (
    with_last_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "product_stock_qty",
        "city_sum_rating",
        "rating_bucket",
        "rating_share",
        "brand",
        F.lag("rating_share").over(w_running).alias("prev_rating_share"),
        F.lead("rating_share").over(w_running).alias("next_rating_share"),
        F.lead("product_stock_qty").over(w_running).alias("next_product_stock_qty"),
        F.lag("product_stock_qty").over(w_running).alias("prev_product_stock_qty"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

result = (
    with_windows
    .withColumn("prev_rating_share_tier", F.when(F.col("prev_rating_share") >= 0.85, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_rating_share_tier", "country"], ["prev_rating_share_tier"], []],
        "prev_rating_share_tier",
        "country",
    )
    .agg(
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.percentile_approx("product_stock_qty", [0.25, 0.5, 0.75]).alias("product_stock_qty_quartiles"),
        F.percentile_approx("rating_bucket", 0.5).alias("rating_bucket_p50"),
    )
    .orderBy(F.col("globex_count").asc_nulls_first())
)
