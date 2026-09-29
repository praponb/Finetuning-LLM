from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("region").orderBy("risk_score", "id")

prepared = (
    df_policyholders
    .filter(F.col("risk_score").between(0.2, 0.7))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        F.year(F.col("joined_on")).alias("joined_on_year"),
        F.substring(F.trim(F.col("holder_name")), 1, 2).alias("holder_name_prefix"),
    )
)

latest = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

region_stats = (
    latest
    .groupBy("region")
    .agg(F.max("joined_on_year").alias("max_joined_on_year"))
)

above_average = (
    latest.alias("s")
    .join(region_stats.alias("g"), F.col("g.region") == F.col("s.region"), "inner")
    .filter(F.col("s.joined_on_year") == F.col("g.max_joined_on_year"))
    .select(
        "s.*",
        "g.max_joined_on_year",
        F.round(F.col("s.joined_on_year") - F.col("g.max_joined_on_year"), 2).alias("joined_on_year_minus_max"),
    )
)

result = (
    above_average
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on_year",
        "holder_name_prefix",
        "max_joined_on_year",
        "joined_on_year_minus_max",
    )
    .orderBy("holder_name_prefix")
)
