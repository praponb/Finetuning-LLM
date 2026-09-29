from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("last4_min_rating", "id")
w3 = Window.orderBy("last4_count_rating", "id")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("city").orderBy(F.col("last4_min_rating").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy(F.col("last4_count_rating_bucket").desc_nulls_first(), "id")
w8 = Window.partitionBy("city").orderBy("product_launch_date", "id")
w9 = Window.partitionBy(F.date_trunc("YEAR", "product_launch_date"), "city")

metrics = (
    df_suppliers
    .filter(F.col("rating") <= 3.3)
    .select(
        "*",
        F.count("rating").over(w_ordered).alias("last4_count_rating"),
        F.max("rating").over(w_ordered).alias("last4_max_rating"),
        F.avg("rating").over(w_ordered).alias("last4_avg_rating"),
        F.min("rating").over(w_ordered).alias("last4_min_rating"),
    )
)

metrics_suppliers = (
    metrics
    .select(
        "id",
        "supplier_name",
        "city",
        "last4_avg_rating",
        F.ntile(3).over(w2).alias("last4_min_rating_bucket"),
        F.ntile(3).over(w3).alias("last4_count_rating_bucket"),
        F.round((F.col("last4_max_rating") / F.sum("last4_max_rating").over(w4)), 4).alias("last4_max_rating_share"),
        F.dense_rank().over(w5).alias("last4_min_rating_dense_rank"),
    )
)

with_last = (
    metrics_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

latest = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

metrics_suppliers_2 = (
    latest
    .withColumn("prev_last4_min_rating_bucket", F.lag("last4_min_rating_bucket").over(w8))
    .withColumn("next_last4_min_rating_bucket", F.lead("last4_min_rating_bucket").over(w8))
    .withColumn("next_last4_count_rating_bucket", F.lead("last4_count_rating_bucket").over(w8))
    .withColumn("prev_last4_count_rating_bucket", F.lag("last4_count_rating_bucket").over(w8))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("last4_count_rating_bucket_share", F.round((F.col("last4_count_rating_bucket") / F.sum("last4_count_rating_bucket").over(w9)), 4))
)

summary = (
    metrics_suppliers_2
    .groupBy("city")
    .agg(
        F.round(F.avg("prev_last4_count_rating_bucket"), 2).alias("avg_prev_last4_count_rating_bucket"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("last4_min_rating_bucket")).otherwise(0)).alias("berlin_last4_min_rating_bucket"),
        F.round(F.sum("prev_last4_count_rating_bucket") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("prev_last4_count_rating_bucket_per_chiang_mai"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.sum("last4_count_rating_bucket_share").alias("total_last4_count_rating_bucket_share"),
    )
)

result = (
    summary
    .select(
        "city",
        "avg_prev_last4_count_rating_bucket",
        "london_count",
        "total_last4_count_rating_bucket_share",
    )
    .orderBy(
        F.col("city").desc(),
        F.col("total_last4_count_rating_bucket_share").desc(),
    )
)
