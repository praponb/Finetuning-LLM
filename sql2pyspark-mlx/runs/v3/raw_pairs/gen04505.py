from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("balance").between(49000, 56000))
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        F.first("balance").over(w).alias("first_balance"),
    )
)
