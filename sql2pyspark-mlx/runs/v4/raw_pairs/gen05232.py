from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "customer_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "account_type", "opened_date", "customer_id")
    .orderBy(F.col("opened_date").desc())
)
