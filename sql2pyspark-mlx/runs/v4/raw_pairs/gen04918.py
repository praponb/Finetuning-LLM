from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "customer_id", "balance", "opened_date")
)
