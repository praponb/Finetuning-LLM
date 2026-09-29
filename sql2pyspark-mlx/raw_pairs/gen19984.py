from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country")
w3 = Window.orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("city").orderBy("prev_rating", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w_grp = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("prev_rating", "id").rowsBetween(-2, Window.currentRow)
w_grp_2 = Window.orderBy("prev_rating", "id").rowsBetween(-3, -1)

metrics = (
    df_suppliers
    .select(
        "*",
        F.ntile(5).over(w).alias("rating_bucket"),
        F.lag("rating", 2).over(w).alias("prev_rating"),
    )
)

enriched = (
    metrics
    .filter(F.col("country").isin(["TH", "JP"]))
    .select(
        "id",
        "rating",
        "prev_rating",
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.ntile(5).over(w3).alias("rating_bucket_2"),
        F.avg("rating_bucket").over(w4).alias("rating_bucket_avg_5"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.stock_qty"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "prev_rating",
        "stock_qty",
        F.sum("stock_qty").over(w_grp).alias("running_stock_qty"),
        F.avg("rating_bucket_avg_5").over(w_hist).alias("rating_bucket_avg_5_avg_2"),
        F.avg("rating_share").over(w_grp_2).alias("rating_share_avg_3_prior"),
    )
)

result = (
    scored
    .select("id", "prev_rating", "stock_qty", "rating_share_avg_3_prior")
    .orderBy(F.col("prev_rating").desc_nulls_first())
)
