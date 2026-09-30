from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance_stddev").desc())

kept_keys = (
    df_accounts
    .select("account_type")
    .exceptAll(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
        .select("account_type")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.city"), F.col("d.signup_date"))
)

eligible = (
    joined
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.signup_date")), "left_semi")
)

joined_accounts = (
    eligible
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(F.lower(F.col("d.name")).like("%avi%"))
    .select(
        "src.customer_id",
        "src.account_type",
        "src.balance",
        "src.status",
        "src.city",
        F.col("d.segment"),
        F.col("d.age"),
        F.col("d.country"),
    )
)

result = (
    joined_accounts
    .groupBy(
        "status",
        F.when(F.col("age") >= 57, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("age_per_credit"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.avg("age").alias("avg_age"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("age")).otherwise(0)).alias("singapore_age"),
    )
    .withColumn("balance_stddev_dense_rank", F.dense_rank().over(w))
    .orderBy("distinct_cities")
)
