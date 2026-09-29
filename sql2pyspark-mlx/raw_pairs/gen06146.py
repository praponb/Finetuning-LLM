from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & ~F.col("status").isin(["CLOSED", "FROZEN"]))
    .withColumn("status_length", F.length("status"))
)

result = (
    base
    .groupBy(
        F.when(F.col("status_length") >= 82, "LARGE").otherwise("SMALL").alias("status_length_tier"),
        "customer_id",
        "status",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min("balance").alias("min_balance"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
    )
    .orderBy("customer_id")
)
