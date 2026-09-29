from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1450, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.count_if(F.col("method") == "card").alias("card_events"),
    )
    .orderBy("card_events")
)
