from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_premium = Window.partitionBy("status").orderBy("premium", "id")
w_status_product = Window.partitionBy("status", "product")
w_all_by_premium_minus_max_tie = Window.orderBy(F.col("premium_minus_max").asc_nulls_last())
w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc())

prepared = (
    df_policies
    .filter((F.col("holder_id") <= 19) & (F.year(F.col("start_date")) == 2024))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
    )
)

holder_id_stats = (
    prepared
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_premium"))
)

compared = (
    prepared.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

with_windows = (
    compared
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "end_date_month",
        "max_premium",
        "premium_minus_max",
        F.row_number().over(w_status_by_premium).alias("premium_seq"),
        F.round(F.col("max_premium") - F.avg("max_premium").over(w_status_product), 2).alias("max_premium_vs_status_product_avg"),
        F.rank().over(w_all_by_premium_minus_max_tie).alias("premium_minus_max_rank"),
        F.dense_rank().over(w_status_holder_id_by_start_da).alias("start_date_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "end_date_month",
        "max_premium",
        "premium_minus_max",
        "premium_seq",
        "max_premium_vs_status_product_avg",
        "premium_minus_max_rank",
        F.greatest(F.col("start_date_dense_rank"), F.col("premium_seq")).alias("greatest_start_date_dense_rank_premium_seq"),
        F.date_format(F.col("end_date_month"), "MMM yyyy").alias("end_date_month_label"),
    )
)
