from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy(F.col("rating").asc_nulls_first())
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc())
w4 = Window.partitionBy("city", "country")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("product_category").orderBy("rating", "id")
w8 = Window.orderBy(F.col("rating").desc(), "id")

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "eco")), "left_anti")
)

scored = (
    kept
    .filter(F.col("country").isin(["TH", "JP", "DE"]))
    .select(
        "*",
        F.ntile(5).over(w).alias("rating_bucket"),
        F.rank().over(w2).alias("rating_rank"),
        F.dense_rank().over(w3).alias("rating_dense_rank"),
        F.avg("rating").over(w4).alias("city_avg_rating"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating",
        "s.rating_bucket",
        "s.rating_dense_rank",
        "s.city_avg_rating",
        "s.product_category",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "rating",
        "rating_bucket",
        "rating_dense_rank",
        "product_category",
        "product_stock_qty",
        F.lag("product_stock_qty").over(w_recent).alias("prev_product_stock_qty"),
        F.lead("product_stock_qty").over(w_recent).alias("next_product_stock_qty"),
        F.lag("rating_bucket").over(w_recent).alias("prev_rating_bucket"),
        F.lead("rating_bucket").over(w_recent).alias("next_rating_bucket"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.ntile(4).over(w8).alias("rating_bucket_2"),
    )
)
