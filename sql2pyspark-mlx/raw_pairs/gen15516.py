from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.sum("balance").over(w_hist).alias("running_sum_balance"),
        F.avg("balance").over(w_hist).alias("running_avg_balance"),
        F.min("balance").over(w_hist).alias("running_min_balance"),
    )
)
