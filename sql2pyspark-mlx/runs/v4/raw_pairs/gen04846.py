from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_part_2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

base = (
    flattened
    .filter(F.col("effective_from") < "2025-03-01")
    .withColumn("effective_from_period", F.date_trunc("YEAR", "effective_from"))
)

kept_keys = (
    base
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "THB"]))
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("element_max_30d", F.max("element").over(w_part))
    .withColumn("element_rank", F.rank().over(w_part_2))
    .withColumn("element_rank_2", F.rank().over(w_part_2))
    .withColumn("element_avg_90d", F.avg("element").over(w3))
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "effective_from_period").alias("year_start"))
    .agg(
        F.percentile_approx("element_rank_2", 0.9).alias("element_rank_2_p90"),
        F.percentile_approx("element", 0.5).alias("element_p50"),
        F.percentile_approx("element_avg_90d", [0.25, 0.5, 0.75]).alias("element_avg_90d_quartiles"),
    )
    .orderBy(F.col("year_start").desc())
)
