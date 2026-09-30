from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

ranked = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "rn",
        F.when(F.col("browser").rlike("^(Safari|Chrome)$"), "EXTENDED").when(F.col("browser").rlike("^(Firefox|Edge)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("browser_zone"),
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").desc())
)
