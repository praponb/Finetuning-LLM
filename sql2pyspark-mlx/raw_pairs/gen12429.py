from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("years_since_shipped_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-05-28").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        (F.datediff(F.current_date(), "shipped_date") / 365.25).alias("years_since_shipped_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "carrier_clean", "latest_rank")
)

valid = (
    latest
    .filter(F.col("latest_rank") != 1)
    .withColumn("latest_rank_scaled", F.round((F.col("latest_rank") * 1.15), 2))
    .withColumn("latest_rank_num", F.col("latest_rank").cast("double"))
)

base = (
    valid
    .filter(F.col("latest_rank_num").between(45, 61))
    .select(
        "*",
        F.least("latest_rank_num", "latest_rank_scaled").alias("least_latest_rank_num_latest_rank_scaled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("least_latest_rank_num_latest_rank_scaled") >= 86, "HIGH").otherwise("LOW").alias("least_latest_rank_num_latest_rank_scaled_tier"),
    )
    .agg(
        F.round(F.avg("latest_rank_scaled"), 2).alias("avg_latest_rank_scaled"),
        F.percentile_approx("least_latest_rank_num_latest_rank_scaled", [0.25, 0.5, 0.75]).alias("least_latest_rank_num_latest_rank_scaled_quartiles"),
    )
)
