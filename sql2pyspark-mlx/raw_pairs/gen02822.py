from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-2, -1)
w2 = Window.orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-02-11")
    .select(
        "id",
        "balance",
        "opened_date",
        F.avg("balance").over(w).alias("balance_avg_2_prior"),
        F.rank().over(w2).alias("balance_rank"),
        F.sum("balance").over(w3).alias("running_balance"),
        F.first("status", ignorenulls=True).over(w4).alias("first_known_status"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("balance") >= 66000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.percentile_approx("balance_avg_2_prior", 0.95).alias("balance_avg_2_prior_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
)
