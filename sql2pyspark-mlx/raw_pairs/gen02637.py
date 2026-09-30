from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").desc_nulls_last(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

latest = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").rlike("^(Chrome|Safari)$"), "CORE").when(F.col("browser").rlike("^(Firefox|Edge)$"), "EXTENDED").otherwise("GROUP_B").alias("browser_zone"),
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("browser").desc_nulls_last())
)
