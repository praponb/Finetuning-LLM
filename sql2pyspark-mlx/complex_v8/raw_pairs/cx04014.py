from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")

base_policys = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

tagged = (
    base_policys
    .filter(F.col("end_date") >= F.lit("2026-02-16").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "product_ref",
        "status_len",
        F.least(F.col("status_len"), F.col("premium")).alias("least_status_len_premium"),
        F.when(F.col("status_len") >= 9, "large").when(F.col("status_len") >= 7, "medium").otherwise("small").alias("status_len_tier"),
        F.concat(F.col("product"), F.lit("#"), F.col("status_len").cast("string")).alias("product_ref_2"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") == "delay"), "left_anti")
    .filter(F.col("product_ref_2").isNotNull())
)

metrics = without_matches.withColumn("rows_in_status", F.count(F.lit(1)).over(w_status))

result = (
    metrics
    .filter(F.col("holder_id") >= 15)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        "least_status_len_premium",
        "status_len_tier",
        "product_ref_2",
        "rows_in_status",
    )
    .orderBy(F.col("id").desc())
)
