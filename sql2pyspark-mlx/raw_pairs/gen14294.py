from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-03-27"))) & (F.col("starts_on") <= "2025-10-01"))
    .withColumn("channel_upper", F.upper("channel"))
    .withColumn("promo_code_title", F.initcap(F.trim("promo_code")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    valid
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.percent_rank().over(w).alias("discount_pct_percent_rank"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
    )
)
