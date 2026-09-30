from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy(F.col("element_scaled").desc_nulls_first())
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan", "currency")
w4 = Window.partitionBy("currency", "plan")
w_recent = Window.partitionBy("currency").orderBy(F.col("element_scaled_cume_dist").desc_nulls_last())
w_recent_2 = Window.partitionBy("currency")
w7 = Window.orderBy(F.col("element_percent_rank").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

cleaned = (
    lines
    .filter(F.col("element") >= 79)
    .withColumn("currency_upper", F.upper("currency"))
    .withColumn("element_scaled", F.round((F.col("element") * 1.1), 2))
)

enriched = (
    cleaned
    .select(
        "id",
        "currency",
        "element",
        "currency_upper",
        F.cume_dist().over(w_grp).alias("element_scaled_cume_dist"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.avg("element").over(w3).alias("plan_avg_element"),
        F.min("element_scaled").over(w4).alias("currency_min_element_scaled"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("element_scaled_cume_dist_cume_dist"),
        F.max("element_scaled_cume_dist").over(w_recent_2).alias("currency_max_element_scaled_cume_dist"),
    )
)

scored = (
    enriched_price_tiers
    .select(
        "currency",
        "element",
        "currency_upper",
        "element_scaled_cume_dist",
        "currency_min_element_scaled",
        F.percent_rank().over(w7).alias("element_percent_rank_percent_rank"),
        F.avg("element_percent_rank").over(w_recent_2).alias("currency_avg_element_percent_rank"),
    )
)

result = (
    scored
    .groupBy("currency")
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.round(F.avg("element_scaled_cume_dist"), 2).alias("avg_element_scaled_cume_dist"),
    )
)
