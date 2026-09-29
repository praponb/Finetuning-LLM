from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

exploded = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("element"))

base = (
    exploded
    .filter(F.col("element").between(70, 77))
    .withColumn("element_tier", F.when(F.col("element") >= 85, "High").when(F.col("element") >= 18, "Medium").otherwise("Low"))
    .withColumn("plan_length", F.length("plan"))
)

cleaned = (
    base
    .filter(F.col("plan").isin(["pro", "basic", "enterprise"]))
    .withColumn("is_basic", F.when(F.col("plan") == "basic", 1).otherwise(0))
    .withColumn("is_basic_2", F.when(F.col("plan") == "basic", 1).otherwise(0))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("is_basic") > 56)
    .select(
        "plan",
        "element",
        "element_tier",
        "is_basic_2",
        F.greatest("is_basic", "plan_length").alias("greatest_is_basic_plan_length"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

enriched = (
    cleaned_price_tiers
    .select(
        "element",
        "element_tier",
        "greatest_is_basic_plan_length",
        "plan_prefix",
        F.min("greatest_is_basic_plan_length").over(w).alias("plan_min_greatest_is_basic_plan_length"),
    )
)

result = (
    enriched
    .select("element", "plan_prefix", "plan_min_greatest_is_basic_plan_length")
    .orderBy(
        F.col("element").desc(),
        F.col("plan_min_greatest_is_basic_plan_length").desc(),
    )
)
