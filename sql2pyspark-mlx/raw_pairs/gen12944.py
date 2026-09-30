from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit_p50").desc())

kept_keys = (
    df_customers
    .select("segment", "country")
    .subtract(
        df_customers
        .filter(F.col("signup_date") < "2024-10-01")
        .select("segment", "country")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "segment",
        F.when(F.col("age") >= 64, "LARGE").otherwise("SMALL").alias("age_tier"),
        F.when(F.col("credit_limit") >= 16400, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("age")).otherwise(0)).alias("uk_age"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.stddev_samp("age").alias("age_stddev"),
    )
    .withColumn("credit_limit_p50_rank", F.rank().over(w))
    .orderBy("age_stddev")
)
