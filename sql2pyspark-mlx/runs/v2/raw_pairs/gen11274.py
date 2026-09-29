from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "balance",
        F.lag("balance", 2).over(w).alias("prev_balance"),
    )
    .orderBy(F.col("opened_date").desc())
)
