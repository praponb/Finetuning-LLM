from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_filled = Window.partitionBy("level_filled")
w_all_by_level_filled_ties = Window.orderBy("level_filled")

base = (
    df_adjusters
    .filter((F.upper(F.trim(F.col("adjuster_name"))).like("F%")) & (F.col("team").isin("Property", "Motor", "Travel")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

windowed = (
    base
    .select(
        "id",
        "adjuster_name",
        "team",
        "adjuster_name_slug",
        "level_filled",
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level_filled), F.lit(0)), 4).alias("level_share_of_level_filled"),
        F.round(F.percent_rank().over(w_all_by_level_filled_ties), 3).alias("level_filled_percent_rank"),
    )
)

result = (
    windowed
    .groupBy("level_filled")
    .pivot("team", ["Health", "Property"])
    .agg(F.avg("level_share_of_level_filled"))
)
