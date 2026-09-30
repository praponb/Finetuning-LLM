from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy(F.col("premium").desc_nulls_first(), "id")
w_all_by_premium_ties = Window.orderBy("premium")

base_policys = (
    df_policies
    .filter(F.col("premium") <= 3521.8)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.when(F.col("premium") >= 2945.9, "large").when(F.col("premium") >= 2013.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

windowed = (
    base_policys
    .select(
        "*",
        F.row_number().over(w_all_by_premium).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
    )
)

detail_rows = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.hour(F.col("c.reported_at")).between(8, 10)), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.days_start_date_to_end_date",
        "s.product_ref",
        "s.premium_tier",
        "s.premium_percent_rank",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

derived = (
    detail_rows
    .select(
        "id",
        "premium",
        "days_start_date_to_end_date",
        "premium_percent_rank",
        "claim_id",
        "claim_date",
        "claim_reported_at",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

result = (
    derived
    .select("id", "premium_percent_rank")
    .distinct()
)
