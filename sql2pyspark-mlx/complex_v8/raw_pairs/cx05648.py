from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_moving_6 = Window.orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w_holder_id_status_or_unknown = Window.partitionBy("holder_id", "status_or_unknown").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_holder_id_status_or_unknown_2 = Window.partitionBy("holder_id", "status_or_unknown").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id")
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_status_or_unknown_3 = Window.partitionBy("holder_id", "status_or_unknown").orderBy(F.col("holder_risk_score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_policies
    .filter(~F.col("product").isin("LIFE", "AUTO"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_details_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status_or_unknown",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    with_details_holder
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status_or_unknown",
        "holder_risk_score",
        F.sum("premium").over(w_all_by_start_date_moving_6).alias("premium_sum_7"),
        F.last("start_date").over(w_holder_id_status_or_unknown).alias("last_start_date"),
        (F.col("premium") - F.lag("premium").over(w_all_by_start_date)).alias("premium_change"),
        F.row_number().over(w_holder_id_status_or_unknown_2).alias("rn"),
        F.nth_value("start_date", 2).over(w_all_by_start_date_full).alias("second_start_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.min("premium").over(w_holder_id_status_or_unknown_3).alias("min_premium_so_far"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.size(F.collect_set("status_or_unknown").over(w_holder_id)).alias("distinct_status_or_unknown_in_holder_id"),
    )
)

leaders = (
    windowed
    .filter((F.col("rn") <= 3) & (F.col("min_premium_so_far").between(2690.0, 32570.0)))
    .drop("rn")
)

result = (
    leaders
    .filter(F.col("premium_share_of_holder_id") <= 0.8)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status_or_unknown",
        "holder_risk_score",
        "last_start_date",
        "premium_change",
        "second_start_date",
        "premium_share_of_holder_id",
        "min_premium_so_far",
        "rows_in_holder_id",
        "distinct_status_or_unknown_in_holder_id",
        (F.col("premium_sum_7") + F.col("min_premium_so_far")).alias("premium_sum_7_plus_min_premium_so_far"),
        F.greatest(F.col("holder_risk_score"), F.col("premium")).alias("greatest_holder_risk_score_premium"),
    )
    .orderBy(F.col("premium").desc())
)
