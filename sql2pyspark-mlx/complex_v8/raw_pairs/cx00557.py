from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("end_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

annotated = base.select("*", F.month(F.col("end_date")).alias("end_date_month"))

enriched = (
    annotated
    .filter(F.col("end_date_month").between(3, 11))
    .select(
        "*",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
        F.when(F.col("product").isin("TRAVEL", "HEALTH"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

with_details_holder = (
    enriched.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.end_date_dayofweek",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_details_holder
    .groupBy("holder_joined_on")
    .pivot("product", ["HEALTH", "HOME", "LIFE", "AUTO"])
    .agg(F.avg("premium"))
)
