from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy(F.month("opened_date"), "customer_id").orderBy("opened_date", "id")
w5 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-6, -1)

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.avg("balance").over(w).alias("balance_avg_6_prior"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("balance").over(w5).alias("balance_avg_6_prior_2"),
    )
)
