from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("starter_tier_value").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.when(F.col("tier_value") >= 89, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.sum(F.when(F.col("plan") == "starter", F.col("tier_value")).otherwise(0)).alias("starter_tier_value"),
    )
    .withColumn("starter_tier_value_rank", F.rank().over(w))
)
