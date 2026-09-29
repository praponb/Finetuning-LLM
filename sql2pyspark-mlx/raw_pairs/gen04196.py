from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("plan", "status").orderBy(F.col("start_date").asc_nulls_last(), "id")

ranked = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.country").isin(["US", "TH", "DE"]))
    .select("src.*", F.col("d.signup_date"))
)

latest = (
    with_dims
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    latest
    .select("customer_id", "id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("start_date") < "2025-03-09")
        .select("customer_id", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .cube(F.col("c.city").alias("city"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.monthly_fee")).alias("avg_monthly_fee"),
    )
    .orderBy(F.col("status").asc_nulls_first())
)
