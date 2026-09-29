from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "opened_date")).orderBy("balance", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w_hist).alias("running_avg_balance"),
        F.sum("balance").over(w_hist).alias("running_sum_balance"),
        F.max("balance").over(w_hist).alias("running_max_balance"),
        F.avg("balance").over(w2).alias("balance_avg_5"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "running_avg_balance",
        "running_sum_balance",
        "balance_avg_5",
    )
    .orderBy("balance_avg_5", F.col("running_avg_balance").desc())
)
