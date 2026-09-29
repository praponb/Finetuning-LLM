from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("element"),
    )
)

base = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-06-06").cast("date"))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

filtered = (
    base
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-06-02"))) & F.month("effective_from").isin([6, 7, 11]))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        "plan_title",
        F.when(F.col("plan").isin(["basic", "enterprise"]), "Priority").otherwise("Other").alias("plan_group"),
        F.length("plan").alias("plan_length"),
    )
)

cleaned = (
    filtered
    .filter((F.col("effective_from") >= F.lit("2024-04-15").cast("date")) & (F.col("element") < 24))
    .select("*", F.round("plan_length", 0).alias("plan_length_rounded"))
)

result = (
    cleaned
    .select("id", "plan", "effective_from", "element", "plan_group", "plan_length")
)
