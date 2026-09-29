from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type")
w2 = Window.partitionBy(F.date_trunc("week", "opened_date"), "customer_id").orderBy("opened_date", "id").rowsBetween(-2, -1)

result = (
    df_accounts
    .select(
        "*",
        F.count("balance").over(w).alias("customer_id_count_balance"),
        F.avg("balance").over(w2).alias("balance_avg_2_prior"),
    )
)
