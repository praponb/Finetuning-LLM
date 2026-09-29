from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id", "claim_type").orderBy(F.col("claimed_amount").desc(), F.col("id").desc())

prepared = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") >= F.lit("2024-04-11").cast("date")), "left_semi")
)

latest_per_key = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupBy("policy_id", "adjuster_id")
    .pivot("claim_type", ["delay", "collision", "medical"])
    .agg(F.avg("claimed_amount"))
)
