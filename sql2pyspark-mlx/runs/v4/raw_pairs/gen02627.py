from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w))
    .withColumn("balance_avg_3", F.avg("balance").over(w2))
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w3))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "balance_avg_3",
        "last_known_status",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
