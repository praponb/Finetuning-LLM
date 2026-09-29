from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("pro_element").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

result = (
    lines
    .groupBy(
        "plan",
        F.when(F.col("element") >= 75, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("plan") == "pro", F.col("element")).otherwise(0)).alias("pro_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("element_per_basic"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
        F.sum("element").alias("total_element"),
    )
    .withColumn("pro_element_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("total_element").desc())
)
