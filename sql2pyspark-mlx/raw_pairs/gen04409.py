from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

enriched = (
    flattened
    .select(
        "plan",
        "effective_from",
        "currency",
        F.percent_rank().over(w).alias("val_percent_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-03-24")))
    .select(
        "*",
        F.substring("currency", 1, 2).alias("currency_prefix"),
        F.round((F.col("val_percent_rank") * 0.85), 2).alias("adjusted_val_percent_rank"),
    )
)

metrics = (
    valid
    .withColumn("plan_avg_adjusted_val_percent_rank", F.avg("adjusted_val_percent_rank").over(w2))
)

valid_price_tiers = (
    metrics
    .filter(F.col("effective_from") >= F.lit("2024-05-07").cast("date"))
    .withColumn("adjusted_val_percent_rank_filled", F.coalesce("adjusted_val_percent_rank", F.lit(0)))
    .withColumn("adjusted_val_percent_rank_rounded", F.round("adjusted_val_percent_rank", 1))
)

result = (
    valid_price_tiers
    .select(
        "plan",
        "adjusted_val_percent_rank_rounded",
        "adjusted_val_percent_rank_filled",
    )
    .unpivot(
        ids=["plan"],
        values=["adjusted_val_percent_rank_rounded", "adjusted_val_percent_rank_filled"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
