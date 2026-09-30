from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("element_filled_int_plus_element_filled_per_element").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("element_filled_int").asc_nulls_first())
w_grp_2 = Window.partitionBy("plan")
w_running = Window.partitionBy("plan").orderBy("element_filled_per_element")
w_hist = Window.orderBy(F.col("element_filled_per_element").desc_nulls_last())

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

valid = (
    lines
    .filter(F.col("element") < 76)
    .select(
        "*",
        F.coalesce("element", F.lit(0)).alias("element_filled"),
        F.trim("plan").alias("clean_plan"),
    )
)

valid_price_tiers = (
    valid
    .filter(F.col("plan") != "starter")
    .select(
        "id",
        "plan",
        "element",
        "clean_plan",
        F.col("element_filled").cast("int").alias("element_filled_int"),
        (F.col("element_filled") / F.nullif("element", F.lit(0))).alias("element_filled_per_element"),
    )
)

valid_price_tiers_2 = (
    valid_price_tiers
    .filter(F.col("plan") == "basic")
    .withColumn("element_filled_int_plus_element_filled_per_element", (F.col("element_filled_int") + F.col("element_filled_per_element")))
)

with_windows = (
    valid_price_tiers_2
    .filter(F.col("plan") == "enterprise")
    .select(
        "*",
        F.percent_rank().over(w_grp).alias("element_filled_int_plus_element_filled_per_element_percent_rank"),
        F.cume_dist().over(w2).alias("element_filled_int_cume_dist"),
        F.min("element_filled_int_plus_element_filled_per_element").over(w_grp_2).alias("plan_min_element_filled_int_plus_element_filled_per_element"),
        F.rank().over(w_running).alias("element_filled_per_element_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "element",
        "clean_plan",
        "element_filled_int_plus_element_filled_per_element",
        "element_filled_int_plus_element_filled_per_element_percent_rank",
        "plan_min_element_filled_int_plus_element_filled_per_element",
        "element_filled_per_element_rank",
        F.rank().over(w_hist).alias("element_filled_per_element_rank_2"),
    )
)

result = (
    metrics
    .select(
        "element",
        "clean_plan",
        "element_filled_int_plus_element_filled_per_element_percent_rank",
        "plan_min_element_filled_int_plus_element_filled_per_element",
        "element_filled_per_element_rank",
    )
)
