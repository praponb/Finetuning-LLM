from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc(), "id")

ranked = (
    df_accounts
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "latest_rank",
        F.when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "GROUP_A").otherwise("PRIORITY").alias("account_type_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
