from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team").orderBy(F.col("level").desc(), F.col("id").desc())

base_adjusters = (
    df_adjusters
    .filter((F.col("team") == "Travel") & (F.upper(F.trim(F.col("adjuster_name"))).like("G%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

latest = (
    base_adjusters
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

qualified = (
    latest
    .crossJoin(latest.agg(F.avg("level_filled").alias("overall_avg_level_filled")))
    .filter(F.col("level_filled") > F.col("overall_avg_level_filled"))
    .drop("overall_avg_level_filled")
)

result = (
    qualified
    .groupingSets(
        [["level_filled", "level"], ["level_filled"], []],
        "level_filled", "level",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("level"), 2).alias("avg_level"),
        F.sum(F.when(F.col("adjuster_name").isNotNull(), F.col("level_filled"))).alias("level_filled_adjuster_name_is_not_null"),
    )
)
