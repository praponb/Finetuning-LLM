from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_holder_id_by_premium_full = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_by_start_date = Window.partitionBy("product").orderBy("start_date", "id")

recent = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

filtered = (
    recent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "CANCELLED"), "left_anti")
)

sequenced = (
    filtered
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.round(F.cume_dist().over(w_holder_id_by_premium), 3).alias("premium_cume_dist"),
        F.nth_value("start_date", 2).over(w_holder_id_by_premium_full).alias("second_start_date"),
        F.row_number().over(w_product_by_start_date).alias("rn"),
    )
)

joined_holder = (
    sequenced.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.year(F.col("p.joined_on")) == 2024)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.premium_cume_dist",
        "s.second_start_date",
        "s.rn",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

labeled = joined_holder.select("*", F.length(F.trim(F.col("product"))).alias("product_len"))

derived = (
    labeled
    .filter(F.col("second_start_date").between(F.lit("2024-04-03").cast("date"), F.lit("2025-09-24").cast("date")))
    .select(
        "id",
        "product",
        "start_date",
        "premium_cume_dist",
        "second_start_date",
        "rn",
        "holder_joined_on",
        "product_len",
        F.when(F.col("product").isin("LIFE", "HOME"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.when(F.col("premium") >= 3817.5, "A").when(F.col("premium") >= 2319.8, "B").otherwise("C").alias("premium_tier"),
    )
)

stats = (
    derived
    .filter(F.col("product_len") > 5)
    .groupBy("product", "rn")
    .agg(
        F.percentile_approx("product_len", 0.5).alias("median_product_len"),
        F.max("rn").alias("max_rn"),
        F.count("*").alias("n_rows"),
        F.sum("premium_cume_dist").alias("total_premium_cume_dist"),
        F.countDistinct("premium_tier").alias("distinct_premium_tiers"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    stats
    .select(
        "product",
        "n_rows",
        "total_premium_cume_dist",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)
