from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-5, -1)
w_recent = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

scored = (
    df_accounts
    .withColumn("balance_avg_5_prior", F.avg("balance").over(w))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w_recent))
    .withColumn("balance_bucket", F.ntile(10).over(w_running))
    .withColumn("first_known_status_2", F.first("status", ignorenulls=True).over(w_recent))
)

result = (
    scored
    .groupBy(
        F.when(F.col("balance_bucket") >= 7, "LARGE").otherwise("SMALL").alias("balance_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance_bucket")).otherwise(0)).alias("credit_balance_bucket"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
