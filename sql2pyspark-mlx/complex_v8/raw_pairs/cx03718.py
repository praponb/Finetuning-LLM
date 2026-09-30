from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_status_status_by_end_d = Window.partitionBy("claim_status", "status").orderBy("end_date_year", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_status_by_claim_re = Window.partitionBy("holder_id", "status").orderBy("claim_reported_at", "id", "claim_id")

base = (
    df_policies
    .filter(F.col("status") == "EXPIRED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.year(F.col("end_date")).alias("end_date_year"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

exploded_detail = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.product_slug",
        "s.end_date_year",
        "s.status_product_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

ranked = (
    exploded_detail
    .select(
        "*",
        F.last("end_date", ignorenulls=True).over(w_claim_status_status_by_end_d).alias("last_known_end_date"),
        F.dense_rank().over(w_holder_id_status_by_claim_re).alias("claim_reported_at_dense_rank"),
        F.row_number().over(w_holder_id_status_by_claim_re).alias("claim_reported_at_seq"),
    )
)

result = (
    ranked
    .rollup("claim_adjuster_id", "claim_id", "end_date_year")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("claim_status").alias("distinct_claim_status"),
        F.max("claim_reported_at").alias("last_claim_reported_at"),
        F.round(F.stddev("claim_claimed_amount"), 2).alias("stddev_claim_claimed_amount"),
    )
    .orderBy(
        F.col("claim_adjuster_id").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("end_date_year").asc_nulls_last(),
    )
)
