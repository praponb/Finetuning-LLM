from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("tier_value")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy(F.year("effective_from"))

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("tier_value"))
)

result = (
    flattened
    .filter(F.year("effective_from") == 2024)
    .select(
        "effective_from",
        "tier_value",
        F.rank().over(w_grp).alias("tier_value_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.count("tier_value").over(w3).alias("effective_from_year_count_tier_value"),
    )
)
