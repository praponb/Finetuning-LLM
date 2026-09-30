from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w_grp = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

metrics = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.cume_dist().over(w_grp).alias("element_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter(F.col("element").between(36, 57))
    .withColumn("currency_prefix", F.substring("currency", 1, 2))
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

result = (
    cleaned
    .select(
        "effective_from",
        "currency",
        "element_cume_dist",
        "currency_prefix",
        "days_since_effective_from",
    )
)
