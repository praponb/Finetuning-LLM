from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").desc())
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "customer_id",
        "opened_date",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.lead("balance", 1).over(w2).alias("next_balance"),
    )
)
