from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        F.avg("tier_value").over(w_grp).alias("currency_avg_tier_value"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-04-24").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.concat_ws("/", "currency", "plan").alias("currency_plan_key"),
    )
)

base = (
    valid
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-02-18")))
    .withColumn("plan_group", F.when(F.col("plan").isin(["basic", "starter"]), "Group A").otherwise("Other"))
    .withColumn("months_since_effective_from", (F.datediff(F.current_date(), "effective_from") / 30))
)

matched_keys = (
    base
    .select("id", "plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from").between("2023-05-29", "2024-10-11"))
        .select("id", "plan")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "currency",
        "tier_value_percent_rank",
        "plan_title",
        "currency_plan_key",
        "months_since_effective_from",
    )
    .orderBy("months_since_effective_from")
)
