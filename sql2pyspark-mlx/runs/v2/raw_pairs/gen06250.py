from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "bank_transfer") & ~F.col("method").isin(["paypal", "bank_transfer", "card"]) & (F.col("method") == "card"))
    .groupBy("status", "method")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum("amount").alias("total_amount"),
    )
)
