from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")

lines = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("element"))

base = (
    lines
    .filter(F.col("currency") == "THB")
    .select("*", F.round((F.col("element") * 1.15), 2).alias("adjusted_element"))
)

metrics = (
    base
    .filter(F.col("element") < 52)
    .withColumn("currency_min_adjusted_element", F.min("adjusted_element").over(w_ordered))
)

matched_keys = (
    metrics
    .select("currency", "id")
    .intersect(
        df_price_tiers
        .filter(F.col("effective_from").between("2023-08-01", "2024-09-24"))
        .select("currency", "id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "currency",
        "currency_min_adjusted_element",
        F.when(F.col("currency").rlike("^(THB|USD)$"), "GROUP_B").when(F.col("currency").rlike("^(EUR)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
    .orderBy(F.col("currency").desc())
)
