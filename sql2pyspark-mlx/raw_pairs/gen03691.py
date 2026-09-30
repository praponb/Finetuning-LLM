from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("currency")
w_hist = Window.partitionBy("plan")

flattened = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

matched_keys = (
    flattened
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") == "USD")
        .select("id", "plan")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .filter(F.col("val") > 70)
    .withColumn("val_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w2))
    .withColumn("plan_max_val", F.max("val").over(w_hist))
)

scored_price_tiers = (
    scored
    .select(
        "id",
        "plan",
        "val",
        "val_percent_rank",
        "plan_max_val",
        F.avg("val_percent_rank").over(w2).alias("currency_avg_val_percent_rank"),
    )
)

result = (
    scored_price_tiers
    .select(
        "plan",
        F.col("val_percent_rank").alias("VAL_PERCENT_RANK_VAL"),
        F.col("val").alias("VAL_VAL"),
    )
    .unpivot(
        ids=["plan"],
        values=["VAL_PERCENT_RANK_VAL", "VAL_VAL"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
