from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month = Window.partitionBy(F.month("end_date"))
w_status_len_by_premium_runnin = Window.partitionBy("status_len").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_len_by_premium = Window.partitionBy("status_len").orderBy(F.col("premium").desc(), "id")
w_status_len_by_premium_full = Window.partitionBy("status_len").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-07-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

holder_id_stats = (
    prepared
    .groupBy("holder_id")
    .agg(F.avg("premium_bucket").alias("avg_premium_bucket"))
)

compared = (
    prepared.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium_bucket") > F.col("g.avg_premium_bucket"))
    .select(
        "s.*",
        "g.avg_premium_bucket",
        F.round(F.col("s.premium_bucket") - F.col("g.avg_premium_bucket"), 2).alias("premium_bucket_minus_avg"),
    )
)

ranked = (
    compared
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "premium_bucket",
        "status_len",
        "avg_premium_bucket",
        "premium_bucket_minus_avg",
        F.round(F.col("premium_bucket_minus_avg") / F.nullif(F.sum("premium_bucket_minus_avg").over(w_end_date_month), F.lit(0)), 4).alias("premium_bucket_minus_avg_share_of_end_date_month"),
        F.first("premium", ignorenulls=True).over(w_status_len_by_premium_runnin).alias("first_known_premium"),
        F.round(F.cume_dist().over(w_status_len_by_premium), 3).alias("premium_cume_dist"),
        F.first("premium").over(w_status_len_by_premium_full).alias("first_premium"),
        F.round(F.col("avg_premium_bucket") - F.avg("avg_premium_bucket").over(w_end_date_month), 2).alias("avg_premium_bucket_vs_end_date_month_avg"),
    )
)

result = (
    ranked
    .filter(F.col("premium_bucket") < 4280)
    .select(
        "id",
        "premium_bucket",
        "status_len",
        "premium_bucket_minus_avg",
        "premium_cume_dist",
        "first_premium",
        "avg_premium_bucket_vs_end_date_month_avg",
        F.round(F.col("first_known_premium") / F.nullif(F.col("avg_premium_bucket"), F.lit(0)), 3).alias("first_known_premium_per_avg_premium_bucket"),
    )
    .orderBy(F.col("id").asc())
)
