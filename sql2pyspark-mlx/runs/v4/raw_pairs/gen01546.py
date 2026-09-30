from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("browser")
w2 = Window.orderBy("credit_limit")
w_grp = Window.orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.orderBy(F.col("credit_limit_dense_rank").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.trim("country").alias("clean_country"),
    )
)

matched_keys = (
    filtered
    .select("customer_id")
    .subtract(
        df_sessions
        .filter(F.to_date("started_at") < "2023-07-01")
        .select("customer_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("id")
    .subtract(
        df_sessions
        .filter(F.col("browser").isin(["Firefox", "Edge", "Safari"]))
        .select("id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.credit_limit"), F.col("d.segment"))
)

metrics = (
    enriched
    .select(
        "*",
        F.min("credit_limit").over(w_part).alias("browser_min_credit_limit"),
        F.rank().over(w2).alias("credit_limit_rank"),
        F.dense_rank().over(w_grp).alias("credit_limit_dense_rank"),
    )
)

metrics_sessions = (
    metrics
    .withColumn("browser_min_credit_limit_sum_7d", F.sum("browser_min_credit_limit").over(w4))
    .withColumn("credit_limit_dense_rank_dense_rank", F.dense_rank().over(w5))
)

result = (
    metrics_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "clean_country",
        "credit_limit",
        "browser_min_credit_limit",
        "credit_limit_rank",
    )
)
