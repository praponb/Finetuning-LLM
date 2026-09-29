from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.avg("balance").over(w).alias("last4_avg_balance"),
        F.min("balance").over(w).alias("last4_min_balance"),
        F.sum("balance").over(w).alias("last4_sum_balance"),
    )
)
