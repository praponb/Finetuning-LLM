from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_start_date_year_holder_id = Window.partitionBy(F.year("start_date"), "holder_id")

scoped_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("premium") <= 3719.7))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
    )
)

sequenced = (
    scoped_policys
    .withColumn("premium_quartile", F.ntile(4).over(w_all_by_premium))
    .withColumn("premium_vs_start_date_year_holder_id_avg", F.round(F.col("premium") - F.avg("premium").over(w_start_date_year_holder_id), 2))
)

line_level = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "APPROVED"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

line_level_2 = (
    line_level.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.claim_id",
        "s.claim_status",
        "s.claim_adjuster_id",
        "s.claim_reported_at",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

result = (
    line_level_2
    .select(
        "id",
        "holder_id",
        "premium",
        "claim_id",
        "claim_status",
        "claim_adjuster_id",
        "claim_reported_at",
        "claim_id_2",
        "claim_reported_at_2",
        F.lower(F.trim(F.col("claim_status"))).alias("claim_status_clean"),
    )
)
