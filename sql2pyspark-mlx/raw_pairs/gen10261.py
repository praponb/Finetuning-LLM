from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

exploded = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

filtered = (
    exploded
    .filter(F.col("plan") == "pro")
    .select("*", F.col("element").cast("int").alias("element_int"))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("plan") == "enterprise")
    .select("*", F.round((F.col("element_int") * 0.9), 2).alias("element_int_scaled"))
)

valid = (
    filtered_price_tiers
    .filter(F.col("element").isNotNull())
    .select(
        "*",
        F.upper("plan").alias("plan_upper"),
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

base = (
    valid
    .filter(F.col("plan").isin(["pro", "enterprise"]))
    .select(
        "id",
        "plan",
        "element",
        "element_int",
        "element_int_scaled",
        "plan_title",
        (F.col("element") + F.col("element_int_scaled")).alias("element_plus_element_int_scaled"),
    )
)

base_price_tiers = (
    base
    .filter(~F.col("plan").isin(["pro", "basic"]))
    .select(
        "*",
        F.when(F.col("element") >= 86, "High").when(F.col("element") >= 40, "Medium").otherwise("Low").alias("element_tier"),
    )
)

with_windows = (
    base_price_tiers
    .filter(F.col("element_int_scaled") >= 62)
    .select(
        "plan",
        "element",
        "element_int",
        F.min("element_int_scaled").over(w).alias("plan_min_element_int_scaled"),
        F.avg("element_int").over(w).alias("plan_avg_element_int"),
        F.stddev_samp("element_plus_element_int_scaled").over(w).alias("plan_stddev_samp_element_plus_element_int_scaled"),
        F.stddev_samp("element_int").over(w).alias("plan_stddev_samp_element_int"),
        F.stddev_samp("element").over(w).alias("plan_stddev_samp_element"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("plan_stddev_samp_element") >= 62, "LARGE").otherwise("SMALL").alias("plan_stddev_samp_element_tier"),
    )
    .agg(
        F.percentile_approx("plan_stddev_samp_element_plus_element_int_scaled", 0.5).alias("plan_stddev_samp_element_plus_element_int_scaled_p50"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    grouped
    .select(
        "plan_stddev_samp_element_tier",
        "plan_stddev_samp_element_plus_element_int_scaled_p50",
        "pro_rows",
    )
    .orderBy(
        F.col("plan_stddev_samp_element_plus_element_int_scaled_p50").asc_nulls_last(),
    )
)
