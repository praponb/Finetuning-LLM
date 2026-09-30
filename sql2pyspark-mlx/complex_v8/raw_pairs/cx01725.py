from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_or_unknown = Window.partitionBy("holder_id", "product_or_unknown").orderBy(F.col("premium").desc(), "id")
w_dedup = Window.partitionBy("holder_id").orderBy("max_premium", "id")

valid = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-10-22").cast("date")) & (F.col("end_date") < F.lit("2026-06-18").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

holder_id_stats = (
    valid
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_premium"))
)

above_average = (
    valid.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

with_trends = (
    above_average
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "product_or_unknown",
        "max_premium",
        "premium_minus_max",
        (F.col("max_premium") - F.lag("max_premium").over(w_holder_id_product_or_unknown)).alias("max_premium_change"),
        F.dense_rank().over(w_holder_id_product_or_unknown).alias("premium_dense_rank"),
        (F.col("premium_minus_max") - F.lag("premium_minus_max").over(w_holder_id_product_or_unknown)).alias("premium_minus_max_change"),
    )
)

latest_per_key = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

detail_rows = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.product_or_unknown",
        "s.premium_minus_max",
        "s.premium_dense_rank",
        "s.premium_minus_max_change",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "claim_id",
        "premium_minus_max",
        "premium_minus_max_change",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["premium_minus_max", "premium_minus_max_change"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "claim_id", "measure")
)
