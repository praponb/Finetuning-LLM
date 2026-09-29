from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("starts_on"), "promo_code").orderBy("discount_pct", "id").rowsBetween(-6, -1)

scored = (
    df_promotions
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("discount_pct_avg_6_prior", F.avg("discount_pct").over(w2))
)

result = (
    scored
    .groupBy("channel")
    .agg(
        F.avg(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct_avg_6_prior"))).alias("welcome5_avg_discount_pct_avg_6_prior"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .filter(F.col("welcome5_avg_discount_pct_avg_6_prior") >= 0)
)
