from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w_part = Window.partitionBy("currency")
w_ordered = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w4 = Window.partitionBy("plan")
w5 = Window.orderBy("element")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

enriched = (
    lines
    .withColumn("element_rank", F.rank().over(w))
    .withColumn("currency_count_element", F.count("element").over(w_part))
    .withColumn("element_dense_rank", F.dense_rank().over(w_ordered))
)

matched_keys = (
    enriched
    .select("currency", "id")
    .exceptAll(
        df_price_tiers
        .filter(~F.col("currency").isin(["THB", "EUR", "USD"]))
        .select("currency", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("currency_count_element_share", F.round((F.col("currency_count_element") / F.sum("currency_count_element").over(w_part)), 4))
    .withColumn("element_dense_rank_share", F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w4)), 4))
    .withColumn("element_cume_dist", F.cume_dist().over(w5))
)

result = (
    scored
    .select(
        "id",
        "currency",
        "element",
        "currency_count_element",
        "element_dense_rank",
        "currency_count_element_share",
        "element_dense_rank_share",
        "element_cume_dist",
        F.when(F.col("currency").rlike("^(THB|USD)$"), "OTHER_ZONE").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
