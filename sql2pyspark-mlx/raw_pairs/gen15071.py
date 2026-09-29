from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("channel", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("channel").isin(["partner", "store", "web"]))
        .select("channel", "id")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("YEAR", "starts_on").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max(F.when(F.col("channel") == "store", F.col("discount_pct"))).alias("store_max_discount_pct"),
    )
)
