from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "Priority").otherwise("Other").alias("status_group"),
        F.col("balance").cast("int").alias("balance_int"),
    )
)
