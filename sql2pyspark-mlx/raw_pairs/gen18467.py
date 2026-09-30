from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("row_count").desc())

enriched = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.credit_limit") < 5000)
    .select(
        "src.id",
        "src.customer_id",
        "src.status",
        F.col("b.signup_date"),
        F.col("b.country"),
        F.col("b.city"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    ranked
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.status",
        "e.country",
        "e.city",
        F.col("d.city").alias("customer_city"),
    )
)

not_flagged = (
    joined
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < "2025-04-01"), "left_semi")
)

result = (
    not_flagged
    .groupBy("status", "country")
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 1)
    .withColumn("row_count_percent_rank", F.percent_rank().over(w2))
)
