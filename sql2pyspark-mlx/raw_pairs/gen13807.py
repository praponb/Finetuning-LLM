from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.orderBy(F.col("val").asc_nulls_last())
w3 = Window.partitionBy("plan", "currency")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

cleaned = (
    lines
    .filter(F.col("val").between(47, 65))
    .withColumn("currency_lower", F.lower("currency"))
    .withColumn("plan_upper", F.upper("plan"))
)

enriched = (
    cleaned
    .filter(F.col("plan") == "starter")
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w))
    .withColumn("val_rank", F.rank().over(w2))
    .withColumn("plan_min_val", F.min("val").over(w3))
)

cleaned_price_tiers = (
    enriched
    .filter(F.col("val") <= 28)
    .select("*", (F.col("plan_min_val") - F.col("val")).alias("plan_min_val_minus_val"))
)

result = (
    cleaned_price_tiers
    .select("currency", "val", "plan_min_val_minus_val")
    .unpivot(
        ids=["currency"],
        values=["val", "plan_min_val_minus_val"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
