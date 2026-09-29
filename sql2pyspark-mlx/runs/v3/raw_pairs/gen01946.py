from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "balance",
        "customer_id",
        F.lag("opened_date", 1).over(w).alias("prev_opened_date"),
        F.first("customer_id").over(w2).alias("first_customer_id"),
    )
    .orderBy("account_type")
)
