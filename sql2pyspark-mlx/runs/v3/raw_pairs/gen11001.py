from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("amount") > 550) & F.col("method").isin(["bank_transfer", "cash"]))
)

summary = (
    recent_payments
    .groupBy(
        F.when(F.col("method").isin(["bank_transfer", "cash"]), "tier_1").otherwise("other").alias("method_group"),
        "status",
    )
    .agg(F.count("*").alias("cnt"))
)

result = (
    summary
    .filter(F.col("cnt") > 9)
    .select("method_group", "status", "cnt")
    .orderBy("status")
)
