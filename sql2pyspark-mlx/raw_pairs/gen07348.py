from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

enriched = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("row_seq"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("balance_cume_dist") >= 0.88, "PREMIUM").otherwise("STANDARD").alias("balance_cume_dist_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("next_balance", [0.25, 0.5, 0.75]).alias("next_balance_quartiles"),
        F.avg("balance_cume_dist").alias("avg_balance_cume_dist"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
    .orderBy(F.col("open_count").asc_nulls_first())
)
