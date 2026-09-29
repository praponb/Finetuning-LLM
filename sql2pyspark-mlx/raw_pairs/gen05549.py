from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

deduped = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "ends_on", "discount_pct")
)

result = (
    deduped
    .groupBy(F.date_trunc("year", "ends_on").alias("year_start"))
    .agg(
        F.sum("discount_pct").alias("total_discount_pct"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
    .filter(F.col("total_discount_pct") > 0)
    .orderBy(F.col("year_start").desc())
)
