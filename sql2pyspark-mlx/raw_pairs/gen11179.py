from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("merchant", "country")
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy(F.year("opened_date"), "country").orderBy("credit_limit")
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("customer_id").orderBy("age_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id", "merchant")
w9 = Window.partitionBy("customer_id").orderBy("credit_limit_avg_3", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.partitionBy("city").orderBy("credit_limit_cume_dist_avg_6", "id")

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        F.col("c.merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

enriched = (
    latest_match
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.opened_date",
        "a.merchant",
        F.col("p.country"),
        F.col("p.email"),
        F.col("p.name"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.signup_date") < "2024-07-01")
    .select("e.*", F.col("p.age"), F.col("p.credit_limit"), F.col("p.city"))
)

enriched_accounts = (
    with_dims
    .select(
        "id",
        "customer_id",
        "merchant",
        "country",
        "email",
        "city",
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_3"),
        F.avg("credit_limit").over(w4).alias("credit_limit_avg_5_prior"),
        F.cume_dist().over(w5).alias("credit_limit_cume_dist"),
        F.stddev_samp("age").over(w6).alias("country_stddev_samp_age"),
    )
)

enriched_accounts_2 = (
    enriched_accounts
    .withColumn("running_credit_limit_cume_dist", F.sum("credit_limit_cume_dist").over(w7))
    .withColumn("customer_id_sum_credit_limit_avg_5_prior", F.sum("credit_limit_avg_5_prior").over(w8))
    .withColumn("credit_limit_cume_dist_avg_6", F.avg("credit_limit_cume_dist").over(w9))
)

latest = (
    enriched_accounts_2
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "merchant",
        "country",
        "email",
        "city",
        "credit_limit_avg_3",
        "credit_limit_cume_dist",
        "running_credit_limit_cume_dist",
        "customer_id_sum_credit_limit_avg_5_prior",
    )
    .orderBy(F.col("city").desc_nulls_last(), F.col("credit_limit_cume_dist").desc())
)
