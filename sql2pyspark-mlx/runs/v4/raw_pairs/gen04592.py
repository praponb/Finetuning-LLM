from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan")

exploded = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("val"))

remaining_keys = (
    exploded
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
        .select("id", "plan")
    )
)

verified = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("val_dense_rank", F.dense_rank().over(w))
    .withColumn("val_dense_rank_2", F.dense_rank().over(w2))
    .withColumn("plan_count_val", F.count("val").over(w3))
)

valid = (
    with_windows
    .filter(F.col("val") < 39)
    .select(
        "*",
        F.round("val_dense_rank_2", 2).alias("val_dense_rank_2_rounded"),
        F.when(F.col("plan").isin(["basic", "pro"]), "Core").otherwise("Other").alias("plan_group"),
    )
)

result = (
    valid
    .select(
        "id",
        "plan",
        "val",
        "val_dense_rank",
        "val_dense_rank_2",
        "plan_count_val",
        "plan_group",
        F.when(F.col("plan").rlike("^(starter|enterprise)$"), "EXTENDED").otherwise("CORE").alias("plan_zone"),
    )
    .orderBy(F.col("val_dense_rank").asc_nulls_first())
)
