from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.first("opened_date").over(w).alias("first_opened_date"),
    )
)
