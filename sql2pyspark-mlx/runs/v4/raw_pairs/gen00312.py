from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("YEAR", "ends_on").alias("year_start"), "channel")
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
)
