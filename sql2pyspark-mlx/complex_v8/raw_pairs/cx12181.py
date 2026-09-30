from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_start_date = Window.partitionBy("status").orderBy("start_date", "id")
w_status_by_start_date_full = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_policys = (
    df_policies
    .filter((F.col("status") == "ACTIVE") & (F.col("start_date") >= F.lit("2025-01-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.abs(F.col("premium") - 2111.3).alias("premium_distance"),
    )
)

filtered = (
    cleaned_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") <= 32441.2), "left_semi")
    .filter(F.col("premium_distance") >= 1500.0)
)

derived = (
    filtered
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "product",
        "start_date",
        "status",
        F.when(F.col("status").isin("ACTIVE", "CANCELLED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.round(F.percent_rank().over(w_status_by_start_date), 3).alias("start_date_percent_rank"),
        F.nth_value("start_date", 2).over(w_status_by_start_date_full).alias("second_start_date"),
        F.first("start_date").over(w_status_by_start_date_full).alias("first_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_status_by_start_date)).alias("days_since_prev_start_date"),
        F.ntile(10).over(w_status_by_start_date).alias("start_date_decile"),
        F.lead("start_date", 2).over(w_status_by_start_date).alias("next2_start_date"),
    )
)

result = (
    ranked
    .select(
        "id",
        "product",
        "start_date",
        "status",
        "start_date_percent_rank",
        "second_start_date",
        "first_start_date",
        "days_since_prev_start_date",
        "next2_start_date",
        F.add_months(F.col("first_start_date"), 3).alias("first_start_date_plus_3m"),
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
    )
)
