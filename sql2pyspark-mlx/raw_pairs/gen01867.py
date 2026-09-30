from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

filtered = (
    flattened
    .filter(F.col("plan") != "basic")
    .select("*", F.col("element").cast("double").alias("element_num"))
)

cleaned = (
    filtered
    .filter(F.col("currency") == "USD")
    .select(
        "currency",
        "element",
        "element_num",
        (F.col("element_num") + F.col("element")).alias("element_num_plus_element"),
        F.when(F.col("plan") == "pro", 1).otherwise(0).alias("is_pro"),
    )
)

valid = (
    cleaned
    .filter(F.col("element_num").between(33, 55))
    .select(
        "*",
        (F.col("is_pro") + F.col("element")).alias("is_pro_plus_element"),
        F.round("element_num", 2).alias("element_num_rounded"),
    )
)

scored = (
    valid
    .filter(F.col("element_num") > 58)
    .select(
        "element",
        "element_num",
        "element_num_plus_element",
        "is_pro",
        "is_pro_plus_element",
        F.stddev_samp("element").over(w).alias("currency_stddev_samp_element"),
    )
)

result = (
    scored
    .select("element_num")
    .orderBy("element_num")
)
