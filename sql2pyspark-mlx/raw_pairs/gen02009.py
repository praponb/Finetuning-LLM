from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct_int").desc(), "id")

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-04-30").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.add_months("starts_on", 3).alias("starts_on_plus_months"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "starts_on", "discount_pct_int", "channel_filled")
)
