from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-10-01")
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        "status",
        (F.col("balance") - F.lag("balance", 2).over(w)).alias("balance_change"),
        F.rank().over(w2).alias("rnk"),
    )
)
