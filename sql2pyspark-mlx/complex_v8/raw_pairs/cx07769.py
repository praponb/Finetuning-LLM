from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), F.col("id").desc())
w_start_date_year_start_date_m = Window.partitionBy("start_date_year", F.date_trunc("month", "start_date"))
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")

recent = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2026-11-25").cast("date"))) & (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.year(F.col("start_date")).alias("start_date_year"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left_semi")
    .filter(F.col("days_start_date_to_end_date") <= 350)
)

scored = (
    without_matches
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "start_date_year",
        "days_start_date_to_end_date",
        F.round(F.col("premium") / F.nullif(F.col("start_date_year"), F.lit(0)), 3).alias("premium_per_start_date_year"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

latest_per_key = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches_2 = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") == "theft"), "left_semi")
)

windowed = (
    without_matches_2
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        "start_date_year",
        "days_start_date_to_end_date",
        "premium_per_start_date_year",
        "product_slug",
        F.round(F.col("days_start_date_to_end_date") / F.nullif(F.sum("days_start_date_to_end_date").over(w_start_date_year_start_date_m), F.lit(0)), 4).alias("days_start_date_to_end_date_share_of_start_date_"),
        F.size(F.collect_set("status").over(w_start_date_year_start_date_m)).alias("distinct_status_in_start_date_year_start_date_mo"),
        F.first("days_start_date_to_end_date", ignorenulls=True).over(w_holder_id_by_premium_running).alias("first_known_days_start_date_to_end_date"),
        F.size(F.collect_set("holder_id").over(w_start_date_year_start_date_m)).alias("distinct_holder_id_in_start_date_year_start_date"),
        F.lag("premium_per_start_date_year", 2).over(w_holder_id_by_premium).alias("prev2_premium_per_start_date_year"),
    )
)

result = (
    windowed
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        "start_date_year",
        "days_start_date_to_end_date",
        "premium_per_start_date_year",
        "product_slug",
        "days_start_date_to_end_date_share_of_start_date_",
        "distinct_status_in_start_date_year_start_date_mo",
        "first_known_days_start_date_to_end_date",
        "distinct_holder_id_in_start_date_year_start_date",
        "prev2_premium_per_start_date_year",
        F.date_add(F.col("end_date"), 90).alias("end_date_plus_90d"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)
