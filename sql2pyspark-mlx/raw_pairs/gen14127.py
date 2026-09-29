from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("country", "product_brand")
w5 = Window.partitionBy("country", "product_brand").orderBy(F.col("stock_qty").desc(), "id")
w6 = Window.partitionBy("country").orderBy(F.col("stock_qty").desc(), "id")
w7 = Window.partitionBy("product_brand", "country").orderBy("running_count_rating", "id")
w8 = Window.partitionBy("country").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_brand", "country").orderBy(F.col("running_avg_rating_share").asc_nulls_last())
w10 = Window.partitionBy("country", "product_brand").orderBy(F.col("running_avg_rating").desc_nulls_last())

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") != 231), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.count("rating").over(w).alias("running_count_rating"),
        F.avg("rating").over(w).alias("running_avg_rating"),
        F.max("rating").over(w).alias("running_max_rating"),
        F.sum("rating").over(w).alias("running_sum_rating"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.running_count_rating",
        "s.running_avg_rating",
        F.col("c.brand").alias("product_brand"),
        F.col("c.stock_qty"),
    )
)

metrics = (
    latest_match
    .filter(F.col("stock_qty") < 352)
    .withColumn("running_avg_rating_share", F.round((F.col("running_avg_rating") / F.sum("running_avg_rating").over(w4)), 4))
    .withColumn("event_seq", F.row_number().over(w5))
)

result = (
    metrics
    .select(
        "id",
        "country",
        "running_count_rating",
        "product_brand",
        "stock_qty",
        "event_seq",
        F.row_number().over(w6).alias("event_seq_2"),
        F.lag("running_avg_rating_share", 1).over(w7).alias("prev_running_avg_rating_share"),
        F.sum("running_count_rating").over(w8).alias("cumulative_running_count_rating"),
        F.dense_rank().over(w9).alias("running_avg_rating_share_dense_rank"),
        F.rank().over(w10).alias("running_avg_rating_rank"),
    )
)
