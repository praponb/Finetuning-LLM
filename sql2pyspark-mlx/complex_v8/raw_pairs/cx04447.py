from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_premium_tier = Window.partitionBy(F.year("end_date"), "premium_tier")
w_product_holder_id_by_premium = Window.partitionBy("product", "holder_id").orderBy("premium")

filtered_policys = (
    df_policies
    .filter(F.col("premium") <= 3521.8)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.when(F.col("premium") >= 4670.9, "A").when(F.col("premium") >= 2289.0, "B").otherwise("C").alias("premium_tier"),
    )
)

windowed = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "premium_tier",
        F.size(F.collect_set("product").over(w_end_date_year_premium_tier)).alias("distinct_product_in_end_date_year_premium_tier"),
        F.round(F.cume_dist().over(w_product_holder_id_by_premium), 3).alias("premium_cume_dist"),
        F.count(F.lit(1)).over(w_end_date_year_premium_tier).alias("rows_in_end_date_year_premium_tier"),
        F.round(F.col("premium") - F.avg("premium").over(w_end_date_year_premium_tier), 2).alias("premium_vs_end_date_year_premium_tier_avg"),
        F.size(F.collect_set("holder_id").over(w_end_date_year_premium_tier)).alias("distinct_holder_id_in_end_date_year_premium_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "flood", 1)).alias("claim_claim_type_count"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_counts = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.first_claim_date",
    )
)

result = (
    with_counts
    .groupingSets(
        [["premium_tier", "distinct_holder_id_in_end_date_year_premium_tier"], ["premium_tier"], []],
        "premium_tier", "distinct_holder_id_in_end_date_year_premium_tier",
    )
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.countDistinct("premium_tier").alias("distinct_premium_tiers"),
    )
)
