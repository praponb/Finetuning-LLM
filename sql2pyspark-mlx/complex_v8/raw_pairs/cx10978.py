from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_bucket = Window.partitionBy("holder_id").orderBy("premium_bucket")

base = (
    df_policies
    .filter((F.col("premium") >= 2319.8) & (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.when(F.col("product").isin("LIFE", "HEALTH"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
    )
)

sequenced = (
    base
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "premium_bucket",
        "product_clean",
        F.round(F.cume_dist().over(w_holder_id_by_premium_bucket), 3).alias("premium_bucket_cume_dist"),
    )
)

screened = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") != "fire"), "left_semi")
    .filter(F.col("premium_bucket") >= 1640)
)

result = (
    screened
    .select(
        "id",
        "premium",
        "end_date",
        "premium_bucket_cume_dist",
        F.upper(F.trim(F.col("product_clean"))).alias("product_clean_clean"),
        F.upper(F.trim(F.col("product_clean"))).alias("product_clean_clean_2"),
    )
)
