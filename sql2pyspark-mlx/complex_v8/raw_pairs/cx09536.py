from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_premium_ties = Window.orderBy("premium")

cleaned_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
    )
)

windowed = (
    cleaned_policys
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.size(F.collect_set("holder_id").over(w_status)).alias("distinct_holder_id_in_status"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.round(F.avg("premium").over(w_status), 2).alias("status_avg_premium"),
        F.dense_rank().over(w_all_by_premium_ties).alias("premium_dense_rank"),
    )
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.min("premium_vs_status_avg").alias("overall_min_premium_vs_status_avg")))
    .filter(F.col("premium_vs_status_avg") > F.col("overall_min_premium_vs_status_avg"))
    .drop("overall_min_premium_vs_status_avg")
)

top_rows = (
    with_activity
    .filter(F.col("premium_dense_rank") <= 5)
    .drop("premium_dense_rank")
)

result = (
    top_rows
    .groupBy("end_date")
    .pivot("status", ["CANCELLED", "ACTIVE", "LAPSED", "EXPIRED"])
    .agg(F.count("premium_vs_status_avg"))
    .orderBy("end_date")
)
