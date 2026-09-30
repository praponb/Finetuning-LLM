from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "status").orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first())

enriched = (
    df_accounts
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w))
    .withColumn("balance_rank", F.rank().over(w2))
    .withColumn("next_balance", F.lead("balance", 2).over(w3))
    .withColumn("cumulative_balance", F.sum("balance").over(w))
    .withColumn("balance_cume_dist", F.cume_dist().over(w4))
)

result = (
    enriched
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "last_known_status",
        "cumulative_balance",
        F.when(F.col("account_type").rlike("^(CREDIT|CHECKING)$"), "GROUP_A").when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "OTHER_ZONE").otherwise("CORE").alias("account_type_zone"),
    )
    .orderBy(F.col("opened_date").desc_nulls_last(), "cumulative_balance")
)
