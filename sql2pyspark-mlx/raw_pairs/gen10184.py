from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("opened_date"), "account_type").orderBy("balance", "id")

scored = (
    df_accounts
    .select(
        "*",
        F.max("balance").over(w_part).alias("running_max_balance"),
        F.sum("balance").over(w_part).alias("running_sum_balance"),
        F.avg("balance").over(w_part).alias("running_avg_balance"),
        F.lag("balance", 1).over(w2).alias("prev_balance"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "running_max_balance",
        "running_sum_balance",
        "prev_balance",
    )
)
