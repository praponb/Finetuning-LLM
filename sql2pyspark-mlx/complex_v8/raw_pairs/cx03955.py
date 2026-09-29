from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_days_sin = Window.partitionBy("status", "holder_id").orderBy("days_since_start_date", "id")
w_holder_id = Window.partitionBy("holder_id")

cleaned_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2027-03-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date_2"),
    )
)

derived = (
    cleaned_policys
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date_3"),
    )
)

windowed = (
    derived
    .select(
        "*",
        F.lag("premium").over(w_status_holder_id_by_days_sin).alias("prev_premium"),
        F.lag("days_since_start_date").over(w_status_holder_id_by_days_sin).alias("prev_days_since_start_date"),
        F.row_number().over(w_status_holder_id_by_days_sin).alias("seq_no"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.round(F.col("days_since_start_date_2") / F.nullif(F.sum("days_since_start_date_2").over(w_holder_id), F.lit(0)), 4).alias("days_since_start_date_2_share_of_holder_id"),
    )
)

enriched_holder = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.days_since_start_date",
        "s.days_since_start_date_2",
        "s.days_since_start_date_3",
        "s.prev_premium",
        "s.prev_days_since_start_date",
        "s.seq_no",
        "s.premium_share_of_holder_id",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    enriched_holder
    .cube("status", "seq_no", "holder_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("holder_risk_score"), 2).alias("stddev_holder_risk_score"),
        F.round(F.sum("days_since_start_date_2") / F.nullif(F.sum("days_since_start_date"), F.lit(0)), 3).alias("days_since_start_date_2_to_days_since_start_date"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
