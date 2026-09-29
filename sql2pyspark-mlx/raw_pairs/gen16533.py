from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("rating_bucket").desc())
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("country").orderBy("rating_bucket", "id")
w_hist = Window.orderBy("rating_bucket", "id")
w_part = Window.orderBy("prev_rating_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("seq_no").desc(), "id")
w9 = Window.orderBy(F.col("rating_bucket").desc())
w10 = Window.orderBy("running_avg_next_running_rating", "id").rowsBetween(-2, -1)
w11 = Window.orderBy("running_count_next_running_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy(F.col("seq_no_bucket").desc_nulls_last())

valid = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Chiang Mai"]))
    .select(
        "*",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

metrics = (
    valid
    .filter(~F.col("country").isin(["UK", "SG"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "city_clean",
        F.ntile(10).over(w).alias("rating_bucket"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "supplier_name",
        "rating_bucket",
        "running_rating",
        F.dense_rank().over(w3).alias("rating_bucket_dense_rank"),
        F.min("rating_bucket").over(w4).alias("country_min_rating_bucket"),
        F.lag("rating_bucket", 2).over(w5).alias("prev_rating_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.lead("rating_bucket").over(w_hist).alias("next_rating_bucket"),
        F.lead("running_rating").over(w_hist).alias("next_running_rating"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "rating_bucket",
        F.sum("next_running_rating").over(w_part).alias("running_sum_next_running_rating"),
        F.avg("next_running_rating").over(w_part).alias("running_avg_next_running_rating"),
        F.count("next_running_rating").over(w_part).alias("running_count_next_running_rating"),
        F.ntile(4).over(w8).alias("seq_no_bucket"),
    )
)

scored_suppliers = (
    enriched
    .select(
        "id",
        "running_sum_next_running_rating",
        "running_avg_next_running_rating",
        "running_count_next_running_rating",
        F.cume_dist().over(w9).alias("rating_bucket_cume_dist"),
        F.avg("seq_no_bucket").over(w10).alias("seq_no_bucket_avg_2_prior"),
        F.last("running_avg_next_running_rating", ignorenulls=True).over(w11).alias("last_known_running_avg_next_running_rating"),
        F.dense_rank().over(w12).alias("seq_no_bucket_dense_rank"),
    )
)

result = (
    scored_suppliers
    .select(
        "id",
        "running_sum_next_running_rating",
        "running_count_next_running_rating",
        "seq_no_bucket_avg_2_prior",
    )
    .orderBy(F.col("id").desc(), "running_count_next_running_rating")
)
