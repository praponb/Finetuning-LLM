from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

scored = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.count("balance").over(w).alias("last3_count_balance"),
        F.min("balance").over(w).alias("last3_min_balance"),
        F.sum("balance").over(w).alias("last3_sum_balance"),
        F.max("balance").over(w).alias("last3_max_balance"),
    )
)

result = (
    scored
    .withColumn("last3_min_balance_tier", F.when(F.col("last3_min_balance") >= 86000, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("year", "opened_date"))
    .cube("last3_min_balance_tier", "account_type", "year_start")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("last3_min_balance_tier")
)
