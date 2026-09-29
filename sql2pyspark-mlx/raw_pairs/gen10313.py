from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["Starbucks", "Amazon"]), "core").otherwise("other").alias("merchant_group"),
        "merchant",
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"),
    )
    .filter(F.col("grab_count") > 51)
    .select("merchant_group", "merchant", "amount_range", "grab_count")
    .orderBy(F.col("merchant_group").desc())
)
