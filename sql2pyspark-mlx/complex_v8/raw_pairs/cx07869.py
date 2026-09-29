from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")

filtered = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "premium_adj",
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_status), 2).alias("premium_adj_vs_status_avg"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date").between(F.lit("2024-05-14").cast("date"), F.lit("2025-03-31").cast("date"))), "left_semi")
    .filter(F.col("product") == "AUTO")
)

result = (
    with_activity
    .groupingSets(
        [["product", "holder_id"], ["product"], ["holder_id"]],
        "product", "holder_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("product") != "HEALTH", 1)) / F.count("*"), 1).alias("pct_product_health"),
        F.min("end_date").alias("first_end_date"),
        F.sum(F.when(F.col("product") == "LIFE", 1).otherwise(0)).alias("n_product_life"),
    )
    .orderBy(F.col("product").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
