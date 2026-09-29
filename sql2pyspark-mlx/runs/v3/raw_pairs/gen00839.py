from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select(
        "id",
        "account_type",
        "opened_date",
        "customer_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "account_type", "opened_date", "customer_id")
    .orderBy("customer_id")
)
