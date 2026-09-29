from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code").orderBy("days_since_starts_on", "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("discount_pct", "id")

filtered = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-06-22").cast("date")) & (F.year("ends_on") >= 2025))
    .select(
        "*",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
    )
)

result = (
    filtered
    .withColumn("channel_avg_days_since_starts_on", F.avg("days_since_starts_on").over(w))
    .withColumn("days_since_starts_on_bucket", F.ntile(4).over(w2))
    .withColumn("last_known_days_since_starts_on", F.last("days_since_starts_on", ignorenulls=True).over(w3))
    .withColumn("discount_pct_bucket", F.ntile(3).over(w4))
)
