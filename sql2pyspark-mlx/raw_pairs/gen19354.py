from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc(), "id")

enriched = (
    df_accounts
    .select(
        "*",
        F.count("balance").over(w_grp).alias("last3_count_balance"),
        F.sum("balance").over(w_grp).alias("last3_sum_balance"),
        F.min("balance").over(w_grp).alias("last3_min_balance"),
        F.ntile(3).over(w_part).alias("balance_bucket"),
    )
)

result = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.percentile_approx("last3_count_balance", 0.5).alias("last3_count_balance_p50"),
    )
)
