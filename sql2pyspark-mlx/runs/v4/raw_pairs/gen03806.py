from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy("val")
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("val").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

valid = (
    lines
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-06-23")))
    .select(
        "*",
        F.lower("currency").alias("currency_lower"),
        F.when(F.col("val") >= 71, "High").when(F.col("val") >= 20, "Medium").otherwise("Low").alias("val_level"),
    )
)

with_windows = (
    valid
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_rank_2", F.rank().over(w2))
    .withColumn("val_rank_3", F.rank().over(w3))
)

remaining_keys = (
    with_windows
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") > "2023-11-22")
        .select("currency")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "plan",
        "val",
        "currency_lower",
        "val_rank",
        F.when(F.col("plan").rlike("^(enterprise|starter)$"), "CORE").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "val")
)
