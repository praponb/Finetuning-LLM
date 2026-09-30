from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy("premium_adj", "id")
w_latest_2 = Window.partitionBy("claim_type").orderBy(F.col("end_date").desc(), F.col("id").desc(), F.col("claim_id").desc())

prepared_policys = (
    df_policies
    .filter(F.col("end_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
    )
)

screened = (
    prepared_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") >= F.lit("2024-08-07").cast("date")), "left_semi")
    .filter(F.col("product_slug").isNotNull())
)

latest = (
    screened
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details = (
    latest.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2024-12-21").cast("date"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.premium_adj",
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
    )
)

with_children = (
    with_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.premium_adj",
        "s.holder_joined_on",
        "s.holder_name",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_type",
    )
)

latest_per_key = (
    with_children
    .withColumn("recency_rank", F.row_number().over(w_latest_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "product",
        "end_date",
        "holder_joined_on",
        "holder_name",
        "claim_adjuster_id",
        "claim_type",
        F.abs(F.col("premium_adj") - 4945.0).alias("premium_adj_distance"),
        F.coalesce(F.col("premium_adj"), F.lit(0.0)).alias("premium_adj_filled"),
    )
    .orderBy(F.col("end_date").desc(), "holder_joined_on")
)
