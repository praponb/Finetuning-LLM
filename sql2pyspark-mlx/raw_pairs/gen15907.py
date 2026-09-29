from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("age_p95").desc())

kept_keys = (
    df_customers
    .select("country", "city")
    .subtract(
        df_customers
        .filter(~F.col("country").isin(["US", "TH"]))
        .select("country", "city")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        "city",
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
    )
    .withColumn("age_p95_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("quarter_start").desc())
)
