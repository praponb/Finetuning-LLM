from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy(F.col("element").asc_nulls_last())
w4 = Window.partitionBy("currency")
w_grp = Window.orderBy(F.col("element_rank_2_share").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

enriched = (
    exploded
    .filter(F.col("element") >= 31)
    .select(
        "id",
        "currency",
        F.rank().over(w).alias("element_rank"),
        F.rank().over(w2).alias("element_rank_2"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.round((F.col("element_rank_2") / F.sum("element_rank_2").over(w4)), 4).alias("element_rank_2_share"),
        F.round((F.col("element_rank_2") / F.sum("element_rank_2").over(w4)), 4).alias("element_rank_2_share_2"),
    )
)

scored = (
    metrics
    .withColumn("element_rank_2_share_percent_rank", F.percent_rank().over(w_grp))
)

result = (
    scored
    .select(
        "currency",
        "element_rank_2_share",
        "element_percent_rank",
        "element_rank_2_share_percent_rank",
        "element_rank_2_share_2",
    )
    .unpivot(
        ids=["currency"],
        values=["element_rank_2_share", "element_percent_rank", "element_rank_2_share_percent_rank", "element_rank_2_share_2"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
)
