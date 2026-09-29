from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w2 = Window.orderBy(F.col("element").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

with_windows = (
    lines
    .select(
        "plan",
        "effective_from",
        "element",
        F.rank().over(w).alias("element_rank"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
    )
)

kept_keys = (
    with_windows
    .select("plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("plan")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("plan")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("element") >= 71, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("element"))).alias("basic_total_element"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.percentile_approx("element_percent_rank", 0.95).alias("element_percent_rank_p95"),
    )
    .orderBy(F.col("element_percent_rank_p95").desc_nulls_first())
)
