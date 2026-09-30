from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product_count_start_d = Window.partitionBy("policy_product_count", F.year("start_date"))
w_start_date_year_by_max_premi = Window.partitionBy(F.year("start_date")).orderBy(F.col("max_premium").asc_nulls_last(), "id")
w_latest = Window.partitionBy("status").orderBy(F.col("max_premium").desc(), F.col("id").desc())

prepared_policys = (
    df_policies
    .filter((F.col("holder_id") <= 7) & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.when(F.col("product").isin("LIFE", "TRAVEL"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
)

product_group_stats = (
    prepared_policys
    .groupBy("product_group")
    .agg(F.max("premium").alias("max_premium"))
)

vs_group = (
    prepared_policys.alias("s")
    .join(product_group_stats.alias("g"), F.col("g.product_group") == F.col("s.product_group"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("start_date").alias("first_policy_start_date"),
        F.count(F.when(F.col("product").isNotNull(), 1)).alias("policy_product_count"),
        F.count("*").alias("policy_count"),
    )
)

with_counts = (
    vs_group.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.first_policy_start_date",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

ranked = (
    with_counts
    .withColumn("policy_product_count_start_date_year_max_premium", F.max("premium").over(w_policy_product_count_start_d))
    .withColumn("max_premium_decile", F.ntile(10).over(w_start_date_year_by_max_premi))
)

latest_per_key = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "product_group",
        "max_premium",
        "premium_minus_max",
        "policy_product_count",
        "policy_product_count_start_date_year_max_premium",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("premium_minus_max").asc())
)
