from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "account_type",
        F.first("account_type").over(w).alias("first_account_type"),
    )
)
