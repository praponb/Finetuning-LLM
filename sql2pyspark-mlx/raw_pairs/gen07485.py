from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

cleaned = (
    lines
    .filter(~F.col("plan").isin(["pro", "basic", "starter"]))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

valid = (
    cleaned
    .filter(F.col("plan").isin(["basic", "enterprise", "starter"]))
    .withColumn("currency_plan", F.concat("currency", F.lit(" - "), "plan"))
)

filtered = (
    valid
    .filter(F.col("element") < 55)
    .select(
        "id",
        "currency",
        "element",
        "plan_title",
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("element") >= 67, "HIGH").otherwise("LOW").alias("element_tier"),
        "currency",
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.sum("element").alias("total_element"),
    )
    .filter(F.col("total_element") > 0)
    .orderBy(F.col("num_currencies").asc_nulls_first())
)
