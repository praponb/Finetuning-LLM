from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")

deduped = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.34, "HIGH").otherwise("LOW"))
    .cube("discount_pct_tier")
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.count("*").alias("row_count"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.grouping("discount_pct_tier").alias("gid"),
    )
    .filter(F.col("avg_discount_pct") >= 0)
    .orderBy(F.col("row_count").desc())
)
