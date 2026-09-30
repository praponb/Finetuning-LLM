from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("balance") < 17000)
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        F.lead("opened_date", 1).over(w).alias("next_opened_date"),
        F.ntile(10).over(w2).alias("bucket"),
    )
    .orderBy("account_type")
)
