from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id", "product").orderBy("max_claim_claimed_amount", "id")
w_holder_id_by_last_claim_repo = Window.partitionBy("holder_id").orderBy("last_claim_reported_at")
w_product = Window.partitionBy("product")
w_product_holder_id_by_last_cl = Window.partitionBy("product", "holder_id").orderBy(F.col("last_claim_reported_at").desc_nulls_first(), "id")
w_holder_id_by_last_claim_repo_2 = Window.partitionBy("holder_id").orderBy("last_claim_reported_at", "id")
w_dedup_2 = Window.partitionBy("product", "status").orderBy(F.col("last_claim_reported_at_cume_dist").desc(), F.col("id").desc())

scoped_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

summarized_rows = (
    scoped_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.max_claim_claimed_amount", "c.last_claim_reported_at")
)

filtered = (
    summarized_rows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") >= F.lit("2024-10-22").cast("date")) & (F.col("p.status").isin("EXPIRED", "LAPSED", "ACTIVE")), "left_anti")
)

latest_per_key = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    latest_per_key
    .select(
        "*",
        F.round(F.cume_dist().over(w_holder_id_by_last_claim_repo), 3).alias("last_claim_reported_at_cume_dist"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.lag("end_date").over(w_product_holder_id_by_last_cl).alias("prev_end_date"),
        F.lead("last_claim_reported_at").over(w_holder_id_by_last_claim_repo_2).alias("next_last_claim_reported_at"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
    )
)

latest_per_key_2 = (
    sequenced
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

holder_id_stats = (
    latest_per_key_2
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_premium"))
)

above_average = (
    latest_per_key_2.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

result = (
    above_average
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "max_claim_claimed_amount",
        "last_claim_reported_at",
        "last_claim_reported_at_cume_dist",
        "prev_end_date",
        "next_last_claim_reported_at",
        "premium_share_of_product",
        (F.floor(F.col("max_claim_claimed_amount") / 10000) * 10000).alias("max_claim_claimed_amount_bucket"),
    )
    .orderBy(
        F.col("last_claim_reported_at_cume_dist").desc_nulls_first(),
        "max_claim_claimed_amount",
    )
)
