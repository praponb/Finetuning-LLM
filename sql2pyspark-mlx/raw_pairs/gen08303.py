from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "city").orderBy("rating")
w_recent = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("last_known_rating").desc(), "id")
w5 = Window.partitionBy("country").orderBy("rating_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("rating_dense_rank", "id").rowsBetween(-3, -1)
w7 = Window.orderBy("running_min_rating_dense_rank", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("country").orderBy("running_max_rating_dense_rank_avg_3_prior", "id").rowsBetween(-3, -1)
w9 = Window.orderBy("running_min_rating_dense_rank_avg_3", "id")
w10 = Window.partitionBy("country").orderBy(F.col("running_min_rating_dense_rank_avg_3").desc(), "id")
w_part = Window.partitionBy("country")
w_ordered = Window.partitionBy("country").orderBy("rating_dense_rank", "id")

enriched = (
    df_suppliers
    .select(
        "*",
        F.first("city", ignorenulls=True).over(w).alias("first_known_city"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
        F.last("rating", ignorenulls=True).over(w_recent).alias("last_known_rating"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select("id", "supplier_name", "country", "rating_dense_rank")
)

metrics = (
    latest
    .select(
        "*",
        F.min("rating_dense_rank").over(w5).alias("running_min_rating_dense_rank"),
        F.count("rating_dense_rank").over(w5).alias("running_count_rating_dense_rank"),
        F.max("rating_dense_rank").over(w5).alias("running_max_rating_dense_rank"),
    )
)

metrics_suppliers = (
    metrics
    .filter(F.col("running_min_rating_dense_rank").isNotNull())
    .withColumn("running_max_rating_dense_rank_avg_3_prior", F.avg("running_max_rating_dense_rank").over(w6))
    .withColumn("running_min_rating_dense_rank_avg_3", F.avg("running_min_rating_dense_rank").over(w7))
)

result = (
    metrics_suppliers
    .select(
        "*",
        F.avg("running_max_rating_dense_rank_avg_3_prior").over(w8).alias("running_max_rating_dense_rank_avg_3_prior_avg_3_prior"),
        F.row_number().over(w9).alias("event_seq"),
        F.row_number().over(w10).alias("seq_no"),
        F.stddev_samp("running_count_rating_dense_rank").over(w_part).alias("country_stddev_samp_running_count_rating_dense_rank"),
        F.lag("running_max_rating_dense_rank_avg_3_prior", 2).over(w_ordered).alias("prev_running_max_rating_dense_rank_avg_3_prior"),
    )
)
