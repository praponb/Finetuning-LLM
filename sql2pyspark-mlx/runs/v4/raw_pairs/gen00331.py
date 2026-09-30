from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc())
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

metrics = (
    exploded
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w2))
    .withColumn("currency_avg_val", F.avg("val").over(w3))
    .withColumn("val_rank", F.rank().over(w))
)

cleaned = (
    metrics
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-10-15"))) & (F.col("currency_avg_val") >= 50))
    .select(
        "id",
        "effective_from",
        "currency",
        "val_percent_rank",
        "val_percent_rank_2",
        "val_rank",
        F.greatest("currency_avg_val", "val_rank").alias("greatest_currency_avg_val_val_rank"),
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

result = (
    cleaned
    .groupBy("currency")
    .agg(
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("currency") == "THB", F.col("greatest_currency_avg_val_val_rank"))).alias("thb_total_greatest_currency_avg_val_val_rank"),
    )
)
