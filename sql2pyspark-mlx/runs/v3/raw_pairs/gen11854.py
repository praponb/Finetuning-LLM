from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([6, 19]))
    .select(
        "id",
        "account_type",
        "opened_date",
        "customer_id",
        "balance",
        F.ntile(3).over(w).alias("quartile"),
        F.lead("balance", 1).over(w2).alias("next_balance"),
    )
    .orderBy(F.col("customer_id").desc())
)
