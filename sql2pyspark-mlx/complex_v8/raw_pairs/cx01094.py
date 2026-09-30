from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id", "status").orderBy(F.col("start_date").desc(), F.col("id").desc())

scoped = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "EXPIRED", "LAPSED")) & (F.col("premium") >= 2013.8))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

latest = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

holder_id_stats = (
    latest
    .groupBy("holder_id")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

compared = (
    latest.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

with_parent = (
    compared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.product_prefix",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

filtered = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") >= 0.0), "left_anti")
)

result = (
    filtered
    .groupBy("holder_id", "holder_segment")
    .pivot("status", ["EXPIRED", "CANCELLED"])
    .agg(F.sum("premium"))
    .orderBy("holder_id", "holder_segment")
)
