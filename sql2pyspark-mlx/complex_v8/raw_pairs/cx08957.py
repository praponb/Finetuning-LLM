from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("premium_tier").orderBy(F.col("start_date").desc(), F.col("id").desc())
w_latest = Window.partitionBy("status_group").orderBy(F.col("start_date").desc(), F.col("id").desc())

recent_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-01-20").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.when(F.col("status").isin("CANCELLED", "EXPIRED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.when(F.col("premium") >= 2945.9, "A").when(F.col("premium") >= 2319.8, "B").otherwise("C").alias("premium_tier"),
    )
)

without_matches = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region").isNotNull()) & (F.col("p.joined_on") < F.lit("2025-07-02").cast("date")), "left_semi")
)

tagged = (
    without_matches
    .select(
        "id",
        "premium",
        "start_date",
        "product_ref",
        "status_group",
        "premium_tier",
        F.when(F.col("premium") >= 3465.4, "HIGH").when(F.col("premium") >= 2111.3, "MEDIUM").otherwise("LOW").alias("premium_tier_2"),
        F.concat_ws("-", F.col("status_group"), F.col("premium_tier")).alias("status_group_premium_tier_label"),
    )
)

latest_per_key = (
    tagged
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    latest_per_key
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "premium",
        "start_date",
        "product_ref",
        "status_group",
        "premium_tier_2",
        "status_group_premium_tier_label",
    )
    .orderBy(F.col("product_ref").desc(), "id")
    .limit(5)
)
