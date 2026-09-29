from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("element_plus_element_whole").desc())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

filtered = (
    exploded
    .filter(F.col("currency") == "THB")
    .select("*", F.ceil("element").alias("element_whole"))
)

cleaned = (
    filtered
    .filter(~F.col("plan").isin(["pro", "starter"]))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "element_whole",
        (F.col("element") + F.col("element_whole")).alias("element_plus_element_whole"),
        (F.col("element") + F.col("element_whole")).alias("element_plus_element_whole_2"),
    )
)

scored = (
    cleaned
    .select(
        "element_plus_element_whole",
        "element_plus_element_whole_2",
        F.cume_dist().over(w).alias("element_plus_element_whole_cume_dist"),
        F.count("element_whole").over(w2).alias("plan_count_element_whole"),
        F.min("element_plus_element_whole_2").over(w3).alias("currency_min_element_plus_element_whole_2"),
    )
)

base = (
    scored
    .filter(F.col("element_plus_element_whole_2") < 21)
    .select(
        "element_plus_element_whole",
        "element_plus_element_whole_cume_dist",
        "plan_count_element_whole",
        "currency_min_element_plus_element_whole_2",
        (F.col("element_plus_element_whole_2") + F.col("plan_count_element_whole")).alias("element_plus_element_whole_2_plus_plan_count_element_whole"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("element_plus_element_whole_cume_dist") >= 0.87, "HIGH").otherwise("LOW").alias("element_plus_element_whole_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("element_plus_element_whole", [0.25, 0.5, 0.75]).alias("element_plus_element_whole_quartiles"),
        F.round(F.avg("element_plus_element_whole"), 2).alias("avg_element_plus_element_whole"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
    )
    .orderBy(F.col("avg_element_plus_element_whole").asc_nulls_last())
)
