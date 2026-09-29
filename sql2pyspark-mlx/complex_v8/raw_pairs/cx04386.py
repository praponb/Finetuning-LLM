from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy("premium_distance", "id")
w_latest_2 = Window.partitionBy("status").orderBy(F.col("premium_distance").desc(), F.col("id").desc())

base_policys = (
    df_policies
    .filter(F.col("holder_id") >= 17)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2824.5).alias("premium_distance"),
    )
)

latest_per_key = (
    base_policys
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details = (
    latest_per_key.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.premium_distance",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

deduped = (
    with_details
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    deduped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") >= F.lit("2024-07-18").cast("date")) & (F.col("c.adjuster_id") >= 3), "left_anti")
)

screened = (
    filtered
    .crossJoin(filtered.agg(F.max("premium_distance").alias("overall_max_premium_distance")))
    .filter(F.col("premium_distance") < F.col("overall_max_premium_distance"))
    .drop("overall_max_premium_distance")
)

result = (
    screened
    .select(
        "id",
        "holder_id",
        "status",
        "premium_distance",
        "holder_segment",
    )
    .orderBy(F.col("holder_segment").desc_nulls_first(), "holder_id")
)
