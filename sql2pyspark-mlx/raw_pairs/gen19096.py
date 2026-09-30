from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date_plus_68d", "id")
w2 = Window.partitionBy(F.month("opened_date_plus_68d"), "account_type").orderBy("opened_date_plus_68d", "id").rowsBetween(-5, -1)

base = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]))
    .select("*", F.date_add("opened_date", 68).alias("opened_date_plus_68d"))
)

result = (
    base
    .filter(F.col("balance") < 72000)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "opened_date_plus_68d",
        F.row_number().over(w).alias("seq_no"),
        F.avg("balance").over(w2).alias("balance_avg_5_prior"),
    )
)
