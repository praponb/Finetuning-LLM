from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city")
w3 = Window.orderBy(F.col("seq_no").desc(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("seq_no").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("seq_no").desc_nulls_last())
w6 = Window.partitionBy("city").orderBy(F.col("prev_sqft_share").desc(), "id")
w7 = Window.orderBy("city_sum_seq_no", "id").rowsBetween(-3, -1)

metrics = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        F.lag("sqft").over(w_hist).alias("prev_sqft"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

with_windows = (
    metrics
    .filter(F.col("seq_no") < 3)
    .select(
        "id",
        "store_name",
        "city",
        F.sum("seq_no").over(w2).alias("city_sum_seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
        F.rank().over(w5).alias("seq_no_rank"),
        F.round((F.col("prev_sqft") / F.sum("prev_sqft").over(w2)), 4).alias("prev_sqft_share"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows_stores = (
    deduped
    .select(
        "*",
        F.max("event_seq").over(w2).alias("city_max_event_seq"),
        F.avg("seq_no_rank").over(w7).alias("seq_no_rank_avg_3_prior"),
    )
)

result = (
    with_windows_stores
    .select(
        "id",
        "store_name",
        "city",
        "city_sum_seq_no",
        "event_seq",
        "row_seq",
        "prev_sqft_share",
        "seq_no_rank_avg_3_prior",
        F.when(F.col("city").rlike("^(New York|Singapore)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy("city")
)
