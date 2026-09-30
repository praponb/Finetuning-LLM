from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "account_type",
        F.lead("opened_date", 1).over(w).alias("next_opened_date"),
        F.dense_rank().over(w2).alias("opened_date_dense_rank"),
    )
)
