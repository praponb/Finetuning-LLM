from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_distanc = Window.partitionBy("holder_id").orderBy(F.col("premium_distance").desc(), "id")
w_status = Window.partitionBy("status")
w_holder_id_by_premium_distanc_2 = Window.partitionBy("holder_id").orderBy(F.col("premium_distance").desc(), "id").rowsBetween(-2, -1)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())

filtered = (
    df_policies
    .filter((F.col("product") == "HOME") & (F.col("premium") > 2033.8))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.abs(F.col("premium") - 2721.8).alias("premium_distance"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.row_number().over(w_holder_id_by_premium_distanc).alias("seq_no"),
        F.size(F.collect_set("holder_id").over(w_status)).alias("distinct_holder_id_in_status"),
        F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_status), 2).alias("premium_distance_vs_status_avg"),
        F.round(F.avg("premium_distance").over(w_holder_id_by_premium_distanc_2), 2).alias("premium_distance_avg_2_prior"),
        (F.col("premium_distance") - F.lag("premium_distance").over(w_all_by_end_date)).alias("premium_distance_change"),
        F.rank().over(w_all_by_end_date_ties).alias("end_date_rank"),
    )
)

filtered_2 = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.claim_date") < F.lit("2024-08-20").cast("date")), "left_anti")
)

result = (
    filtered_2
    .filter(F.col("distinct_holder_id_in_status") > 4)
    .select(
        "id",
        "status",
        "status_clean",
        "premium_distance",
        "seq_no",
        "distinct_holder_id_in_status",
        "premium_distance_vs_status_avg",
        "premium_distance_avg_2_prior",
        "premium_distance_change",
        "end_date_rank",
    )
    .orderBy(F.col("premium_distance_change").asc())
)
