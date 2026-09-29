from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w2 = Window.orderBy(F.col("val").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

enriched = (
    flattened
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w2))
)

base = (
    enriched
    .filter(F.col("plan") == "enterprise")
    .withColumn("plan_upper", F.upper("plan"))
)

kept_keys = (
    base
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "starter")
        .select("id", "plan")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

pivoted = (
    verified
    .groupBy("currency")
    .pivot("plan", ["pro", "starter"])
    .agg(F.sum("val_percent_rank_2").alias("total"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    pivoted
    .select(
        "currency",
        "pro_total",
        "starter_total",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "PRIORITY").when(F.col("currency").rlike("^(EUR)$"), "EXTENDED").otherwise("CORE").alias("currency_zone"),
    )
)
