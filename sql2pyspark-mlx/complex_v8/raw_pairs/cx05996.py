from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .filter((F.col("tariff").isin("TOU", "FLAT")) & (F.col("rate") <= 6.1))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "tariff",
        "rate",
        "tariff_ref",
        F.abs(F.col("rate") - 3.7).alias("rate_distance"),
        F.upper(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown_2"),
    )
)

totals = (
    enriched
    .groupBy("tariff_or_unknown_2", "tariff")
    .agg(F.array_sort(F.collect_set("tariff")).alias("tariff_list"))
)

labeled = (
    totals
    .select(
        "tariff_or_unknown_2",
        "tariff",
        "tariff_list",
        F.array_join(F.array_sort(F.col("tariff_list")), ",").alias("tariff_list_text"),
        F.substring(F.trim(F.col("tariff_or_unknown_2")), 1, 4).alias("tariff_or_unknown_2_prefix"),
        F.when(F.col("tariff_or_unknown_2").isin("TOU", "FLAT"), "group_a").when(F.col("tariff_or_unknown_2") == "EV", "group_b").otherwise("other").alias("tariff_or_unknown_2_group"),
    )
)

exploded = (
    labeled
    .select(
        "tariff_or_unknown_2",
        "tariff",
        "tariff_list_text",
        "tariff_or_unknown_2_prefix",
        "tariff_or_unknown_2_group",
        F.explode_outer("tariff_list").alias("tariff_list_item"),
    )
)

result = (
    exploded
    .groupingSets(
        [["tariff_list_item", "tariff_or_unknown_2"], ["tariff_list_item"]],
        "tariff_list_item", "tariff_or_unknown_2",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(~F.col("tariff").isin("TOU", "EV"), 1).otherwise(0)).alias("n_tariff_not_in_tou_ev"),
    )
    .select(
        F.coalesce(F.col("tariff_list_item"), F.lit("ALL")).alias("tariff_list_item"),
        F.coalesce(F.col("tariff_or_unknown_2"), F.lit("ALL")).alias("tariff_or_unknown_2"),
        "row_count",
        "n_tariff_not_in_tou_ev",
    )
)
