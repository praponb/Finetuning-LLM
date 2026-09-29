from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city").orderBy("rating", "id")
w_grp = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.partitionBy("country", "city").orderBy("rating", "id")
w5 = Window.partitionBy("country", "city")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("city")
w8 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w9 = Window.partitionBy("city").orderBy(F.col("prev_rating").desc(), "id")
w10 = Window.orderBy("row_seq_2_share", "id")
w11 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w12 = Window.partitionBy("city", "product_brand").orderBy(F.col("row_seq_2").desc(), "id")
w13 = Window.partitionBy("category", "country").orderBy(F.col("row_seq_3").desc())
w14 = Window.orderBy("event_seq", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("rating") <= 2.6)
    .select(
        "*",
        F.upper("country").alias("country_upper"),
        F.upper("city").alias("city_upper"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("row_seq_2", F.row_number().over(w_grp))
    .withColumn("prev_rating", F.lag("rating", 1).over(w4))
)

enriched_suppliers = (
    enriched
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
        F.round((F.col("row_seq_2") / F.sum("row_seq_2").over(w6)), 4).alias("row_seq_2_share"),
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share_2"),
    )
)

with_windows = (
    enriched_suppliers
    .select(
        "*",
        F.stddev_samp("row_seq_2").over(w7).alias("city_stddev_samp_row_seq_2"),
        F.row_number().over(w8).alias("row_seq_3"),
        F.row_number().over(w9).alias("event_seq"),
        F.row_number().over(w10).alias("row_seq_4"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w11))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"), F.col("c.category"))
)

metrics = (
    with_last
    .withColumn("row_seq_2_bucket", F.ntile(3).over(w12))
    .withColumn("row_seq_3_percent_rank", F.percent_rank().over(w13))
    .withColumn("event_seq_avg_2", F.avg("event_seq").over(w14))
)

result = (
    metrics
    .select(
        "id",
        "country",
        "city",
        "country_upper",
        "city_upper",
        "row_seq_2",
        "prev_rating",
        "rating_share",
        "row_seq_2_share",
    )
)
