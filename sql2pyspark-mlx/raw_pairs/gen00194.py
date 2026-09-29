from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("region", "city").orderBy(F.col("sqft").asc_nulls_first())
w4 = Window.partitionBy("region").orderBy(F.col("running_sum_sqft").desc_nulls_first())
w5 = Window.partitionBy("city").orderBy(F.col("running_sum_sqft").desc(), "id")
w6 = Window.partitionBy("region").orderBy(F.col("running_count_sqft").desc(), "id")
w7 = Window.partitionBy("region")
w8 = Window.partitionBy("city").orderBy("sqft", "id")
w9 = Window.partitionBy("city").orderBy(F.col("running_min_sqft_share").desc_nulls_first(), "id")
w10 = Window.partitionBy("region").orderBy("running_min_sqft", "id")
w11 = Window.partitionBy("region").orderBy("running_min_sqft", "id").rowsBetween(-5, -1)
w_hist = Window.partitionBy("region").orderBy("running_count_sqft_bucket", "id")

valid = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-03-07").cast("date"))
    .withColumn("store_name_city_key", F.concat_ws(",", "store_name", "city"))
)

metrics = (
    valid
    .select(
        "*",
        F.sum("sqft").over(w).alias("running_sum_sqft"),
        F.max("sqft").over(w).alias("running_max_sqft"),
        F.min("sqft").over(w).alias("running_min_sqft"),
        F.count("sqft").over(w).alias("running_count_sqft"),
        F.rank().over(w2).alias("sqft_rank"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "store_name_city_key",
        "running_sum_sqft",
        "running_min_sqft",
        "running_count_sqft",
        "sqft_rank",
    )
)

scored = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w4).alias("running_sum_sqft_dense_rank"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(10).over(w6).alias("running_count_sqft_bucket"),
        F.round((F.col("running_min_sqft") / F.sum("running_min_sqft").over(w7)), 4).alias("running_min_sqft_share"),
        F.row_number().over(w8).alias("row_seq_2"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "region",
        "store_name_city_key",
        "running_sum_sqft",
        "running_min_sqft",
        "running_count_sqft_bucket",
        "running_min_sqft_share",
        "row_seq_2",
    )
)

scored_stores = (
    deduped
    .filter(F.col("running_count_sqft_bucket").between(6, 8))
    .select(
        "*",
        F.row_number().over(w10).alias("event_seq"),
        F.avg("running_min_sqft").over(w11).alias("running_min_sqft_avg_5_prior"),
        F.ntile(10).over(w_hist).alias("running_count_sqft_bucket_bucket"),
    )
)

remaining_keys = (
    scored_stores
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("sqft").isNotNull())
        .select("id", "region")
    )
)

verified = (
    scored_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "region",
        "store_name_city_key",
        "running_sum_sqft",
        "running_min_sqft",
        "running_count_sqft_bucket",
        "running_min_sqft_share",
        "running_min_sqft_avg_5_prior",
        "running_count_sqft_bucket_bucket",
    )
)
