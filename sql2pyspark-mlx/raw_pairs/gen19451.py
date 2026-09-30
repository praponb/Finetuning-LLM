from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        (F.col("balance") - F.lag("balance", 1).over(w)).alias("balance_change"),
        F.lag("opened_date", 1).over(w2).alias("prev_opened_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
