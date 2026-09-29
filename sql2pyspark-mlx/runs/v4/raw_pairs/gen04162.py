from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .withColumn("balance_bucket", F.ntile(10).over(w))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w2))
    .withColumn("running_balance", F.sum("balance").over(w3))
    .withColumn("cumulative_balance", F.sum("balance").over(w4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("cumulative_balance") >= 346573, "PREMIUM").otherwise("STANDARD").alias("cumulative_balance_tier"),
        F.when(F.col("first_known_balance") >= 86000, "HIGH").otherwise("LOW").alias("first_known_balance_tier"),
    )
    .agg(
        F.percentile_approx("first_known_balance", [0.25, 0.5, 0.75]).alias("first_known_balance_quartiles"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("first_known_balance")).otherwise(0)).alias("credit_first_known_balance"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.max("balance").alias("max_balance"),
    )
)
