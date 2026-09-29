from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("channel").isNotNull())
    .select(
        "*",
        F.quarter("ends_on").alias("ends_on_quarter"),
        (F.datediff(F.current_date(), "ends_on") / 365).alias("years_since_ends_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
