from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("id")
    .subtract(
        df_promotions
        .filter(~F.col("channel").isin(["mobile", "partner"]))
        .select("id")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "channel",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "GROUP_B").when(F.col("channel").rlike("^(web|store)$"), "GROUP_A").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
