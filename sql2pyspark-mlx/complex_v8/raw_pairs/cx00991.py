from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids = Window.partitionBy("distinct_ids")
w_distinct_ids_holder_id_by_di = Window.partitionBy("distinct_ids", "holder_id").orderBy(F.col("distinct_ids_per_max_premium").desc())

cleaned = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-08-27").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.year(F.col("start_date")).alias("start_date_year"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") > 0.2), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.start_date_year",
        F.col("p.region").alias("holder_region"),
    )
)

stats = (
    joined
    .filter(F.col("holder_id").isNotNull())
    .groupBy("holder_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("premium").alias("max_premium"),
    )
    .filter(F.col("max_premium") > 1132.1)
)

filtered = (
    stats
    .crossJoin(stats.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

enriched = (
    filtered
    .filter(F.col("holder_id").isNotNull())
    .select(
        "*",
        F.round(F.col("max_premium") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("max_premium_per_distinct_ids"),
        F.round(F.col("distinct_ids") / F.nullif(F.col("max_premium"), F.lit(0)), 3).alias("distinct_ids_per_max_premium"),
    )
)

with_trends = (
    enriched
    .withColumn("distinct_holder_id_in_distinct_ids", F.size(F.collect_set("holder_id").over(w_distinct_ids)))
    .withColumn("distinct_ids_per_max_premium_rank", F.rank().over(w_distinct_ids_holder_id_by_di))
)

result = (
    with_trends
    .select(
        "holder_id",
        "max_premium",
        "max_premium_per_distinct_ids",
        F.round(F.col("max_premium") * 1.5, 2).alias("max_premium_adj"),
    )
)
