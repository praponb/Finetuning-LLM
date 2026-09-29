from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("element_whole").desc_nulls_first())

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("element"))

cleaned = (
    flattened
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .select(
        "*",
        F.round("element", 2).alias("element_rounded"),
        F.ceil("element").alias("element_whole"),
    )
)

filtered = (
    cleaned
    .filter(F.col("element") < 46)
    .select(
        "id",
        "currency",
        "element_rounded",
        "element_whole",
        F.when(F.col("element") >= 88, "High").when(F.col("element") >= 32, "Medium").otherwise("Low").alias("element_band"),
    )
)

base = (
    filtered
    .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
    .select(
        "*",
        F.floor("element_rounded").alias("element_rounded_whole"),
        F.when(F.col("currency").isin(["THB", "USD"]), "Priority").otherwise("Other").alias("currency_group"),
    )
)

enriched = (
    base
    .filter(F.col("element_rounded_whole") < 63)
    .select(
        "currency",
        "element_rounded",
        "element_band",
        F.cume_dist().over(w_running).alias("element_whole_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "currency",
        "element_band",
        F.when(F.col("currency").rlike("^(EUR|THB)$"), "GROUP_B").otherwise("GROUP_A").alias("currency_zone"),
    )
    .orderBy(F.col("currency").asc_nulls_first(), "currency_zone")
)
