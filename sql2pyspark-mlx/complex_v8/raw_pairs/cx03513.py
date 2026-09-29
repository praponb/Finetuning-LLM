from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_start_da = Window.partitionBy("holder_id", "status").orderBy("start_date", "id")
w_holder_id_status_by_start_da_2 = Window.partitionBy("holder_id", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc(), "id")
w_status_holder_id_by_start_da_3 = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc())
w_all_by_n_status_in_active_la = Window.orderBy("n_status_in_active_lapsed", "start_date_ym", "second_end_date_year")
w_all_by_n_status_in_active_la_2 = Window.orderBy("n_status_in_active_lapsed", "start_date_ym", "second_end_date_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_status_in_active_la_3 = Window.orderBy("n_status_in_active_lapsed", "start_date_ym", "second_end_date_year").rowsBetween(-3, -1)

cleaned = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-12-03").cast("date")) & (F.col("end_date") < F.lit("2026-06-18").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        F.round(F.cume_dist().over(w_holder_id_status_by_start_da), 3).alias("start_date_cume_dist"),
        F.nth_value("end_date", 2).over(w_holder_id_status_by_start_da_2).alias("second_end_date"),
        F.first("start_date", ignorenulls=True).over(w_status_holder_id_by_start_da).alias("first_known_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_holder_id_by_start_da_2)).alias("days_since_prev_start_date"),
        F.lag("start_date", 3).over(w_holder_id_status_by_start_da).alias("prev3_start_date"),
        F.dense_rank().over(w_status_holder_id_by_start_da_3).alias("start_date_dense_rank"),
    )
)

qualified = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") == "collision"), "left_semi")
    .filter(F.year(F.col("second_end_date")) == 2025)
)

stats = (
    qualified
    .filter(F.year(F.col("prev3_start_date")) == 2024)
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        F.year("second_end_date").alias("second_end_date_year"),
    )
    .agg(
        F.count(F.when(F.col("status").isin("ACTIVE", "LAPSED"), 1)).alias("n_status_in_active_lapsed"),
        F.sum("days_since_prev_start_date").alias("total_days_since_prev_start_date"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("n_status_cancelled"),
    )
)

derived = (
    stats
    .filter(F.col("n_status_in_active_lapsed").between(2, 8))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("start_date_ym")), " ", "_")).alias("start_date_ym_slug"),
        (F.col("n_status_cancelled") - F.col("second_end_date_year")).alias("n_status_cancelled_minus_second_end_date_year"),
        F.initcap(F.trim(F.col("start_date_ym"))).alias("start_date_ym_clean"),
    )
)

enriched = (
    derived
    .filter(F.col("start_date_ym").isNotNull())
    .withColumn("start_date_ym_clean_clean", F.lower(F.trim(F.col("start_date_ym_clean"))))
)

ranked_2 = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_n_status_in_active_la), 3).alias("n_status_in_active_lapsed_percent_rank"),
        F.last("n_status_in_active_lapsed", ignorenulls=True).over(w_all_by_n_status_in_active_la_2).alias("last_known_n_status_in_active_lapsed"),
        F.max("second_end_date_year").over(w_all_by_n_status_in_active_la_3).alias("second_end_date_year_max_3_prior"),
    )
)

result = (
    ranked_2
    .filter(F.col("n_status_in_active_lapsed").between(1, 6))
    .select(
        "start_date_ym",
        "n_status_in_active_lapsed",
        "total_days_since_prev_start_date",
        "start_date_ym_slug",
        "n_status_cancelled_minus_second_end_date_year",
        "start_date_ym_clean",
        "start_date_ym_clean_clean",
        "last_known_n_status_in_active_lapsed",
        "second_end_date_year_max_3_prior",
    )
    .orderBy("start_date_ym_slug")
)
