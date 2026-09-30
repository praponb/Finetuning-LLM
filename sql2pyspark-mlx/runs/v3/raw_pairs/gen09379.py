from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .filter(F.col("opened_date") > "2024-10-14")
    .select(
        "id",
        "account_type",
        "opened_date",
        "customer_id",
        F.first("customer_id").over(w).alias("first_customer_id"),
    )
)
