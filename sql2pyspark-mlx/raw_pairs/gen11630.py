from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "CREDIT") & F.hour("txn_time").between(12, 16) & F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        F.when(F.col("txn_type").isin(["FEE", "CREDIT"]), "Group A").otherwise("Other").alias("txn_type_group"),
        F.round("amount", 2).alias("amount_rounded"),
    )
    .orderBy("txn_type", "id")
    .limit(5)
)
