from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.year(F.col("start_date")) == 2025) | (F.col("premium") <= 3544.1))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

tagged = (
    cleaned
    .select(
        "id",
        "start_date",
        "status",
        "premium_adj",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.abs(F.col("premium_adj") - 4945.0).alias("premium_adj_distance"),
    )
)

labeled = (
    tagged
    .select(
        "*",
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
        F.when(F.col("product_or_unknown").isin("LIFE", "TRAVEL"), "group_a").when(F.col("product_or_unknown") == "HEALTH", "group_b").otherwise("other").alias("product_or_unknown_group"),
    )
)

result = (
    labeled
    .select("product_or_unknown")
    .distinct()
    .orderBy(F.col("product_or_unknown").desc())
)
