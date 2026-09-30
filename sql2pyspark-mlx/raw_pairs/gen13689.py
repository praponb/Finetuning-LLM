from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id")

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-07-18"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
