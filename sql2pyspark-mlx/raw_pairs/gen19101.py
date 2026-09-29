from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("cnt").desc())

matched_keys = (
    df_accounts
    .select("id", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status") == "OPEN")
        .select("id", "customer_id")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("balance") >= 62000, "HIGH").otherwise("LOW").alias("balance_tier"),
        "account_type",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .withColumn("cnt_dense_rank", F.dense_rank().over(w))
)
