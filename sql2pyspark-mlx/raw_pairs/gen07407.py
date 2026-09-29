from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "opened_date")).orderBy("balance", "id")
w2 = Window.partitionBy(F.date_trunc("week", "opened_date")).orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "account_type").orderBy("balance", "id")

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "status",
        F.ntile(4).over(w).alias("balance_bucket"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.ntile(4).over(w_hist).alias("balance_bucket_2"),
    )
)
