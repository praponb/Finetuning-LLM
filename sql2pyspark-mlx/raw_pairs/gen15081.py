from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 11))
    .withColumn("method_filled", F.coalesce("method", F.lit("unassigned")))
    .orderBy(F.col("order_id").desc())
)
